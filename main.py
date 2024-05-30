from twikit import Client
import time
import psycopg2
import random
import threading
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection information
DB_HOST = 'ep-tight-limit-a6uyk8mk.us-west-2.retooldb.com'
DB_USER = 'retool'
DB_PASSWORD = 'jr1cAFW3ZIwH'
DB_NAME = 'retool'

# Replace these with your actual login credentials
USERNAME = 'bevsterz001'
EMAIL = 'kevinsjobergmulenga@gmail.com'
PASSWORD = 'testTest123'

# List of Twitter usernames you want to get tweets from
usernames = ["adrianmagnussn", "niklaska", "YasmineB93678", "Socdemola", "Ygeman", "strandhall", "kasirga_kadir",
             "RedarLawen", "mattiasvepsa", "JytteGuteland", "MKallifatides", "Mirjaraiha", "VencuVelasquez",
             "magdandersson", "mikaeldamberg", "asawestlund", "NylundWatz", "tegnr", "AnnaVikstrom",
             "AzadehRojhan", "fredrikolovsson", "Charoline", "shekarabi", "LantzGustaf", "GunillaSvantorp",]

# Initialize the client and login
client = Client(language='en-US')
client.login(auth_info_1=USERNAME, auth_info_2=EMAIL, password=PASSWORD)

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    sslmode='require'
)
cursor = conn.cursor()


def get_last_10_tweets(username):
    try:
        user = client.get_user_by_screen_name(username)
        tweets = client.get_user_tweets(user.id, tweet_type='Tweets', count=5)
        profile_picture_url = user.profile_image_url  # Fetch profile picture URL
        return tweets, profile_picture_url
    except Exception as e:
        logging.error(f"Error fetching tweets for {username}: {e}")
        return [], None


def tweet_exists(username, timestamp):
    try:
        cursor.execute(
            "SELECT 1 FROM tweets WHERE username = %s AND timestamp = %s",
            (username, timestamp)
        )
        return cursor.fetchone() is not None
    except psycopg2.OperationalError as e:
        logging.error(f"OperationalError: {e}")
        raise e


def insert_tweet_to_db(username, timestamp, text, profile_picture_url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            if not tweet_exists(username, timestamp):
                cursor.execute(
                    "INSERT INTO tweets (username, timestamp, text, profilepicture) VALUES (%s, %s, %s, %s)",
                    (username, timestamp, text, profile_picture_url)
                )
                conn.commit()
                logging.info(f"Inserted tweet for {username} at {timestamp}")
            else:
                logging.info(f"Tweet for {username} at {timestamp} already exists")
            break
        except psycopg2.OperationalError as e:
            logging.error(f"OperationalError on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise e
        except Exception as e:
            logging.error(f"Error inserting tweet to DB: {e}")
            raise e


def keep_alive(interval=300):
    def run():
        while True:
            try:
                cursor.execute("SELECT 1")
                conn.commit()
            except psycopg2.OperationalError as e:
                logging.error(f"Keep-alive error: {e}")
            time.sleep(interval)

    threading.Thread(target=run, daemon=True).start()


# Call this function once at the start of your application
keep_alive()

while True:
    random.shuffle(usernames)  # Randomize the order of usernames
    for username in usernames:
        logging.info(f"Last 10 tweets from {username}:")
        tweets, profile_picture_url = get_last_10_tweets(username)
        for tweet in tweets:
            logging.info(f"- {tweet.text}")
            insert_tweet_to_db(username, tweet.created_at, tweet.text, profile_picture_url)
        time.sleep(55)  # Pause for 55 seconds between requests to avoid hitting rate limits

    logging.info("Sleeping for 10 minutes...")
    time.sleep(900)  # Sleep for 10 minutes before running the process again

# Close the database connection when the script is terminated
cursor.close()
conn.close()
