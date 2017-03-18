import gc
from os import listdir
from os.path import isfile, join

from dkg.JsonKafkaProducer import JsonKafkaProducer
from dkg.message.CountryMessage import CountryMessage
from dkg.message.TwitterTickerMessage import TwitterTickerMessage
from dkg.message.WikipediaClickstreamMessage import WikipediaClickstreamMessage
from dkg.message.WikipediaPageviewByLangMessage import WikipediaPageviewByLangMessage
from dkg.message.WikipediaPageviewMessage import WikipediaPageviewMessage


def stream_twitter_ticker(producer, topic, filepath):
    count = 0
    with open(filepath, 'r') as lines:
        for line in lines.read().splitlines():
            part = line.split('\t')
            timestamp = part[0]
            ticker = part[1]
            msg = TwitterTickerMessage(timestamp, ticker)
            if count < 10:
                print(msg.toJson())
            producer.send_message(topic, msg)
            count += 1
    return count


def stream_wikipedia_pageview(producer, topic, filepath):
    count = 0
    with open(filepath, 'r') as lines:
        for line in lines.read().splitlines():
            part = line.split('\t')
            timestamp = part[0]
            page = part[1]
            msg = WikipediaPageviewMessage(timestamp, page)
            producer.send_message(topic, msg)
            if count < 10:
                print(msg.toJson())
            count += 1
    return count


def stream_country(producer, topic, filepath):
    count = 0
    print('file is', filepath)
    with open(filepath, 'r') as lines:
        for country in lines.read().splitlines():
            msg = CountryMessage(country)
            producer.send_message(topic, msg)
            if count < 10:
                print(msg.toJson())
            count += 1
    return count


def stream_twitter_election(producer, topic, filepath):
    count = 0
    print('file is', filepath)
    with open(filepath, 'r') as lines:
        for line in lines.read().splitlines():
            producer.send_message(topic, line)
            if count < 10:
                print(line)
            count += 1
    gc.collect()
    return count


def stream_clickstream(producer, topic, filepath):
    count = 0
    print('file is', filepath)
    with open(filepath, 'r') as lines:
        for line in lines.read().splitlines():
            part = line.split('\t')
            prev = part[0]
            curr = part[1]
            type = part[2]
            n = part[3]
            msg = WikipediaClickstreamMessage(prev, curr, type, n)
            producer.send_message(topic, msg)
            if count < 10:
                print(msg.toJson())
            count += 1
    return count


def stream_wikipedia_pageview_by_lang(producer, topic, filepath):
    count = 0
    print('file is', filepath)
    with open(filepath, 'r') as lines:
        for line in lines.read().splitlines():
            part = line.split('\t')
            lang = part[0]
            page = part[1]
            n = part[2]
            m = part[3]
            msg = WikipediaPageviewByLangMessage(lang, page, n, m)
            producer.send_message(topic, msg)
            if count < 10:
                print(line)
            count += 1
    gc.collect()
    return count


def get_files(dir):
    # files = os.listdir(dir)
    return [f for f in listdir(dir) if isfile(join(dir, f))]


if __name__ == '__main__':
    print('>>> streaming...')
    count = 0
    producer = None

    # twitter ticker
    producer = JsonKafkaProducer()
    count = stream_twitter_ticker(producer, 'twitter-ticker-5', 'D:\\cloud\\data\\pkg\\twitter_ticker.txt')

    # wikipedia pageview
    # producer = JsonKafkaProducer()
    # count = stream_wikipedia_pageview(producer, 'wikipedia-pageview-5', 'D:\\cloud\\data\\pkg\\wiki.txt')

    # country
    # producer = JsonKafkaProducer()
    # count = stream_country(producer, 'country-balanced-5', 'D:\\cloud\\data\\synthetic\\country-balanced.txt')
    # count = stream_country(producer, 'country-half-skew-5', 'D:\\cloud\\data\\synthetic\\country-half-skew.txt')
    # count = stream_country(producer, 'country-skew-5', 'D:\\cloud\\data\\synthetic\\country-skew.txt')

    # twitter election day
    # producer = SimpleKafkaProducer()
    # dir = 'D:\\cloud\\data\\twitter\\election_day\\'
    # files = get_files(dir)
    # for file in files:
    #     count += stream_twitter_election(producer, 'twitter-election-5', dir + file)

    # wikipedia clickstream
    # producer = JsonKafkaProducer()
    # count = stream_clickstream(producer, 'wikipedia-clickstream-5', 'D:\\cloud\\data\\wikipedia\\clickstream\\2017_01_en_clickstream.tsv')

    # wikipedia pageview by lang
    # producer = JsonKafkaProducer()
    # dir = 'D:\\cloud\\data\\wikipedia\\pageviews\\'
    # files = get_files(dir)
    # for file in files:
    #     count += stream_wikipedia_pageview_by_lang(producer, 'wikipedia-pageview-by-lang-5', dir + file)

    print('Total emitted line:', count)
    print('>>> done.')
