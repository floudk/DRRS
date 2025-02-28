from datetime import datetime, timedelta
import json
from confluent_kafka import Producer
import random

class NexmarkConfig:
    def __init__(self, config):

        self.tps_warmup_q7 = int(float(config['tps_warmup_q7']))
        self.tps_warmup_q8 = int(float(config['tps_warmup_q8']))
        self.tps_warmup_duration_q7 = int(float(config['tps_warmup_duration_q7']))
        self.tps_warmup_duration_q8 = int(float(config['tps_warmup_duration_q8']))
        self.tps_q7 = int(float(config['tps_q7']))
        self.tps_q8 = int(float(config['tps_q8']))
        self.each_tps = int(float(config['each_tps']))
        self.events_num = int(float(config['events_num']))
        self.person_proportion = int(config['person_proportion'])
        self.auction_proportion = int(config['auction_proportion'])
        self.bid_proportion = int(config['bid_proportion'])

        self.total_proportion = self.person_proportion + self.auction_proportion + self.bid_proportion

        self.num_active_people = int(config['num_active_people'])
        self.avg_person_byte_size = int(config['avg_person_byte_size'])
        self.first_person_id = int(config['first_person_id'])

        self.first_auction_id = int(config['first_auction_id'])
        self.hot_sellers_ratio = int(config['hot_sellers_ratio'])
        self.first_category_id = int(config['first_category_id'])
        self.avg_auction_byte_size = int(config['avg_auction_byte_size'])
    
        self.hot_auction_ratio = int(config['hot_auction_ratio'])
        self.hot_bidders_ratio = int(config['hot_bidders_ratio'])

        self.num_in_flight_auctions = int(config['num_in_flight_auctions'])
        
        self.avg_bid_byte_size = int(config['avg_bid_byte_size'])

def next_string(length):
    letters = 'abcdefghijklmnopqrstuvwxyz'
    return ''.join(random.choice(letters) for i in range(length))

def next_extra(current_size, target_size):
    padding_size = max(0, (target_size - current_size//2)) # a char is 2 bytes
    return next_string(padding_size)



US_STATES = ["AZ", "CA", "ID", "OR", "WA", "WY"]
US_CITIES = ["Phoenix", "Los Angeles", "San Francisco", "Boise", "Portland", "Bend", 
             "Redmond", "Seattle", "Kent", "Cheyenne"]
FIRST_NAMES = ["Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", 
               "Sarah", "Deiter", "Walter"]
LAST_NAMES = ["Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", 
              "Jones", "Noris"]
def create_credit_card_strings():
    credit_card_strings = [f"{i:04d}" for i in range(10000)]
    return credit_card_strings
CREDIT_CARD_STRINGS = create_credit_card_strings()

PERSON_ID_LEAD = 10
NUM_CATEGORIES = 5
AUCTION_ID_LEAD = 10

def get_base_url():
    return "https://www.nexmark.com/{}/{}/{}/item.htm?query=1".format(
        next_string(5),
        next_string(5),
        next_string(5)
    )

CHANNELS_NUMBER = 10000
def create_channel_url_cache():
    cache = []
    for i in range(CHANNELS_NUMBER):
        url = get_base_url()
        if random.randint(0, 9) > 0:
            url += f"&channel_id={abs(i)}"
        cache.append(("channel-{}".format(i), url))
    return cache
HOT_CHANNELS_RATIO = 2

HOT_CHANNELS = ["Google", "Facebook", "Baidu", "Apple"]

CHANNEL_URL_CACHE = create_channel_url_cache()


def next_price():
    return round(10**random.random()*6*100)

def last_base0_person_id(config: NexmarkConfig, event_id):
    epoch = event_id // config.total_proportion
    offset = event_id % config.total_proportion
    if (offset >= config.person_proportion):
        offset = config.person_proportion - 1
    return epoch * config.total_proportion + offset
def next_base0_person_id(config: NexmarkConfig, event_id):
    num_peple = last_base0_person_id(config, event_id) + 1
    active_people = min(num_peple, config.num_active_people)
    n = random.randint(0, active_people + PERSON_ID_LEAD)
    return num_peple - active_people + n


def last_base0_auction_id(config, event_id):
    epoch = event_id // config.total_proportion
    offset = event_id % config.total_proportion
    if offset < config.person_proportion:
        epoch -= 1
        offset = config.auction_proportion - 1
    elif offset >= config.person_proportion + config.auction_proportion:
        offset = config.auction_proportion - 1
    else:
        offset -= config.person_proportion
    return epoch * config.auction_proportion + offset

def next_base0_auction_id(event_id, config):
    min_auction = max(0, last_base0_auction_id(config, event_id) - config.num_in_flight_auctions)
    max_auction = last_base0_auction_id(config, event_id)
    return min_auction + random.randint(0, max_auction - min_auction + AUCTION_ID_LEAD)




def next_aution(event_id, config: NexmarkConfig, timestamp, events_count_so_far):
    def next_auction_length_ms():
        current_event_number = events_count_so_far + 1
        num_events_for_auctions = (config.num_in_flight_auctions * config.total_proportion) // config.auction_proportion
        future_auction = timestamp
        horizon_ms = future_auction - timestamp
        return 1 + random.randint(0, max(int(horizon_ms * 2), 1))

    id = last_base0_auction_id(config, event_id) + config.first_auction_id # Integer
    if random.randint(0, config.hot_sellers_ratio - 1) > 0:
        seller = (last_base0_person_id(config, event_id) // config.hot_sellers_ratio) * config.hot_sellers_ratio
    else:
        seller = next_base0_person_id(config, event_id)
    seller += config.first_person_id

    category = config.first_category_id + random.randint(0, NUM_CATEGORIES - 1)
    initial_bid = next_price()
    #  timestamp = datetime.now().timestamp()
    expires = timestamp + timedelta(milliseconds=next_auction_length_ms()).total_seconds()
    name = next_string(20)
    desc = next_string(100)
    reserve = initial_bid + next_price()
    current_size = len(name) + len(desc) + 8 * 6
    extra = next_extra(current_size, config.avg_auction_byte_size)

    # return f'{{"id":{id},"seller":{seller},"category":{category},"initialBid":{initial_bid},"expires":"{expires}","name":"{name}","description":"{desc}","reserve":{reserve},"extra":"{extra}"}}'

    auction = {
        "id": id,
        "seller": seller,
        "category": category,
        "initialBid": initial_bid,
        "expires": expires,
        "name": name,
        "description": desc,
        "reserve": reserve,
        "extra": extra
    }
    return auction


def next_bid(event_id, config: NexmarkConfig, timestamp):
    if random.randint(0, config.hot_auction_ratio - 1) > 0:
        auction = (last_base0_auction_id(config, event_id) // config.hot_auction_ratio) * config.hot_auction_ratio
    else:
        auction = next_base0_auction_id(event_id, config)
    auction += config.first_auction_id

    if random.randint(0, config.hot_bidders_ratio - 1) > 0:
        bidder = (last_base0_person_id(config, event_id) // config.hot_bidders_ratio) * config.hot_bidders_ratio + 1
    else:
        bidder = next_base0_person_id(config, event_id)
    bidder += config.first_person_id

    price = next_price()
    if random.randint(0, HOT_CHANNELS_RATIO) == 0:
        i = random.randint(0, len(HOT_CHANNELS) - 1)
        channel = HOT_CHANNELS[i]
        url = CHANNEL_URL_CACHE[i][1]
    else:
        channel_and_url = CHANNEL_URL_CACHE[random.randint(0, CHANNELS_NUMBER - 1)]
        channel = channel_and_url[0]
        url = channel_and_url[1]
    
    current_size = 8 * 4
    extra = next_extra(current_size, config.avg_bid_byte_size)

    bid = {
        "auction": auction,
        "bidder": bidder,
        "price": price,
        "channel": channel,
        "url": url,
        "extra": extra
    }
    return bid


def next_person(event_id, config: NexmarkConfig):
    id = last_base0_person_id(config, event_id) + config.first_person_id # Integer
    name = random.choice(FIRST_NAMES) + " " + random.choice(LAST_NAMES) # String
    email = next_string(7) + "@" + next_string(5) + ".com" # String
    credit_card = ''.join([random.choice(CREDIT_CARD_STRINGS) for _ in range(4)])# String
    city = random.choice(US_CITIES)# String
    state = random.choice(US_STATES)# String
    # calculate the size of the person with java serialization
    current_size = (len(name) + len(email) + len(credit_card) + len(city) + len(state)) * 2 + 8
    extra = next_extra(current_size, config.avg_person_byte_size)

    # return the person as json string
    person = {
        "id": id,
        "name": name,
        "email": email,
        "creditCard": credit_card,
        "city": city,
        "state": state,
        "extra": extra
    }
    return person


def next_event(config: NexmarkConfig, event_id, events_count_so_far, producer: Producer):
    rem = event_id % config.total_proportion
    # current milliseconds
    timestamp = datetime.now().timestamp()
    
    topic = "nexmark-"
    if rem < config.person_proportion:
        person = next_person(event_id, config)
        event = {"event_type": 0, "event": person, "timestamp": timestamp}
        topic += "person"
    elif rem < config.person_proportion + config.auction_proportion:
        auction = next_aution(event_id, config, timestamp, events_count_so_far)
        event = {"event_type": 1, "event": auction, "timestamp": timestamp}
        topic += "auction"
    else:
        bid = next_bid(event_id, config, timestamp)
        event = {"event_type": 2, "event": bid, "timestamp": timestamp}
        topic += "bid"
    producer.produce(topic, json.dumps(event))
    return topic

def get_tps(topic, config: NexmarkConfig):
    return config.tps_q7 if topic == "q7" else config.tps_q8

def get_warmup_tps_and_duration(topic, config: NexmarkConfig):
    if topic == "q7":
        return config.tps_warmup_q7, config.tps_warmup_duration_q7
    else:
        return config.tps_warmup_q8, config.tps_warmup_duration_q8