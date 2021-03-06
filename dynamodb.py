import boto3
import time
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import json


def get_client_connection():
    print("Connect to dynamodb")
    return boto3.client("dynamodb",
                        endpoint_url='http://localhost:8000',
                        region_name='eu-north-1',
                        aws_access_key_id='dummy',
                        aws_secret_access_key='dummy')


def get_resource_connection():
    return boto3.resource("dynamodb",
                          endpoint_url='http://localhost:8000',
                          region_name='eu-north-1',
                          aws_access_key_id='dummy',
                          aws_secret_access_key='dummy')


def is_existing_table(client):
    try:
        description = client.describe_table(TableName='Movies')
        print("Table exists", description)
        return True
    except client.exceptions.ResourceNotFoundException as ex:
        print('Table not found', ex)
        return False


# Create table can be part of serverless deploy script
# https://blog.skbali.com/2019/07/setting-dynamodb-ttl-with-lambda-python/
def create_table(client):
    try:
        print("Create table")
        table = client.create_table(
            TableName='Movies',
            KeySchema=[
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'title',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print('create_table response', table)
        print('Set time to live')
        response = client.update_time_to_live(
            TableName='Movies',
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': 'ttl'
            }
        )
        print("update_time_to_live response", response)

        print("Table status:", client.describe_table(TableName='Movies'))
    except client.exceptions.ResourceInUseException:
        print("Table already exists")
        pass


def get_ttl():
    utc_now = datetime.utcfromtimestamp(time.time())
    ttl_time = utc_now + timedelta(days=10)
    ttl = ttl_time.timestamp()
    return int(ttl)


def load_from_file(db):
    with open('movies.json', 'r') as datafile:
        movies = json.load(datafile)
    table = db.Table('Movies')
    for movie in movies:
        item = {
                'year': movie['year'],
                'title': movie['title'],
                'info': movie['info'],
                'ttl': get_ttl()
        }
        print("LOADING", item)
        response = table.put_item(Item=item)
        print("LOADED ITEM", response)


def add_record(db):
    table = db.Table('Movies')
    table.put_item(
        Item={
            'year': 2020,
            'title': 'Birds of pray',
            'info': {
                'plot': 'Bad girls kill bad guys',
                'rating': '****'
            },
            'ttl': get_ttl()
        }
    )


def add_batch(db):
    ttl = get_ttl()
    batch_items = [
        {'year': 2020, 'title': 'The Call of the Wild', 'info': {'rating': '5.8'}, 'ttl': ttl},
        {'year': 2019, 'title': 'Joker', 'info': {'rating': '8.5'}, 'ttl': ttl},
        {'year': 2019, 'title': 'Avenger: Endgame', 'info': {'rating': '8.4'}, 'ttl': ttl},
        {'year': 2019, 'title': 'Spider-Man: Far from Home', 'info': {'rating': '7.5'}, 'ttl': ttl},
        {'year': 2018, 'title': 'Ready Player One', 'info': {'rating': '7.5'}, 'ttl': ttl}
    ]

    request_items = []
    for item in batch_items:
        req = {'PutRequest': {'Item': item}}
        request_items.append(req)

    db.batch_write_item(RequestItems={
        'Movies': request_items
    })


def get_single_item(db):
    table = db.Table('Movies')
    response = table.get_item(
        Key={
            'year': 1999,
            'title': 'The Matrix'
        }
    )
    print("Find one item:", response['Item'])


def list_movies_for_year(db, year):
    print('Movies for year', year)
    table = db.Table('Movies')
    response = table.query(KeyConditionExpression=Key('year').eq(year))
    movies = response['Items']
    for movie in movies:
        print(movie)
    return movies


def count_items_in_table(db):
    table = db.Table('Movies')
    print("Num items in table", table.item_count)



def find_movies_starting_the(db):
    response = db.Table('Movies').query(
        KeyConditionExpression=Key('year').eq(1999) & Key('title').begins_with('The')
    )
    print("Movies starting with 'The'", response['Items'])


def update_data(db):
    table = db.Table('Movies')
    response = table.update_item(
        Key={
            'year': 2020,
            'title': 'The Call of the Wild'},
        UpdateExpression='SET info.rating=:r, info.plot=:p',
        ExpressionAttributeValues={
            ':r': '6.8',
            ':p': 'A sled dog struggles for survival in the wilds of the Yukon.'
        },
        ReturnValues="UPDATED_NEW"
    )
    print("Record updated", response)


def delete_data(db):
    table = db.Table('Movies')
    response = table.delete_item(
        Key={
            'year': 2020,
            'title': 'Birds of pray'
        }
    )
    print('Movie deleted', response)
    movies = list_movies_for_year(db, 2020)
    for movie in movies:
        if movie['title'] == 'Birds of pray':
            raise Exception('Movie was not deleted')


def delete_table(db):
    print("Deleting table")
    table = db.Table('Movies')
    response = table.delete()
    print('Delete table response', response)


def crud_test():
    client = get_client_connection()
    if not is_existing_table(client):
        create_table(client)

    db = get_resource_connection()
    load_from_file(db)
    list_movies_for_year(db, 1999)
    get_single_item(db)
    find_movies_starting_the(db)
    add_record(db)
    add_batch(db)
    update_data(db)
    count_items_in_table(db)
    delete_data(db)
    delete_table(db)


if __name__ == "__main__":
    crud_test()
