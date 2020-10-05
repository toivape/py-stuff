import requests
from http.client import HTTPConnection
import logging
from typing import NamedTuple

# Debug http connection
log = logging.getLogger('urllib3')
log.setLevel(logging.DEBUG)
HTTPConnection.debuglevel = 1


class Movie(NamedTuple):
    year: str
    title: str
    rating: float
    plot: str


def to_account_status(status):
    if status:
        return "Active"
    return "Inactive"


def send_batch(movies):
    post_movies = []
    for movie in movies:
        m = {
            'year': movie.year,
            'title': movie.title,
            'rating': movie.rating,
            'plot': movie.plot,
        }
        post_movies.append(m)

    url = "http://localhost:8080/v1/movies"
    response = requests.post(url=url, json=post_movies, headers={"Content-Type": "application/json"})
    if response.ok:
        print("Request was success", response)
        print("response headers", response.headers)
    else:
        print("Server returned error", response)
        response.raise_for_status()


def send_batch_test():
    batch = [
        Movie(year=2020, title='The Call of the Wild', rating=6.8, plot='A sled dog struggles for survival in the wilds of the Yukon.'),
        Movie(year=2019, title='Joker', rating=8.5, plot='')
    ]
    send_batch(batch)


if __name__ == "__main__":
    send_batch_test()
