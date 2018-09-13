
import time
import textwrap

from tqdm import tqdm


def welcome_in_big_data():
    comments = [
        {'text': 'let\'s start some calculations', 'sleep': 2},
        {'text': 'fetching data', 'sleep': 3},
        {'text': 'still fetching data', 'sleep': 5},
        {'text': 'yup still fetching data', 'sleep': 6},
        {'text': 'ok starting some calculations', 'sleep': 6},
        {'text': 'calculating', 'sleep': 6},
        {'text': 'still calculating', 'sleep': 6},
        {'text': 'and again still calculating', 'sleep': 6},
        {'text': 'please wait', 'sleep': 2},
        {'text': 'just a litte bit more', 'sleep': 6},
        {'text': 'yes this is how it feels', 'sleep': 5},
        {'text': 'to work in big data', 'sleep': 5},
        {'text': 'still interested?!', 'sleep': 5},
        {'text': 'are you sure?!', 'sleep': 5},
        {'text': 'ok :-)', 'sleep': 3},
    ]

    pbar = tqdm(comments, bar_format='{l_bar}{bar}', ncols=100)
    for comment in pbar:
        pbar.set_description(comment['text'].rjust(32))
        time.sleep(comment['sleep'])

    print(textwrap.dedent('''
        ################################
        ################################

        Welcome is the World of Big Data

        ################################
        ################################
    '''))
