from twikit import Client
import time
import psycopg2
import random
# Database connection information
DB_HOST = 'ep-tight-limit-a6uyk8mk.us-west-2.retooldb.com'
DB_USER = 'retool'
DB_PASSWORD = 'jr1cAFW3ZIwH'
DB_NAME = 'retool'

# Replace these with your actual login credentials
USERNAME = 'WorldBevs72474'
EMAIL = 'collab.bevsworld@gmail.com'
PASSWORD = 'testTest123'

# List of Twitter usernames you want to get tweets from
usernames = ["helenebjorklund", "carpebruksort", "IsacssonLars", "backeskog", "linneawickman", "riksdagsmattias",
             "johanbuser", "CarlssonSwe", "JennieNilsson", "adnandibrani", "Aida_Hadzialic", "AnnaCarenS",
             "carinaodebrink", "Niklas1991", "AzraMuranovic", "lenahallengren", "lailanaraghi", "TomasEneroth",
             "Monicahaider", "IdaKarkiainen", "lundhsammeli", "lskold", "walleannas", "johanssonmorgan",
             ]

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
        print(f"Error fetching tweets for {username}: {e}")
        return [], None


def tweet_exists(username, timestamp):
    cursor.execute(
        "SELECT 1 FROM tweets WHERE username = %s AND timestamp = %s",
        (username, timestamp)
    )
    return cursor.fetchone() is not None


def insert_tweet_to_db(username, timestamp, text, profile_picture_url):
    if not tweet_exists(username, timestamp):
        try:
            cursor.execute(
                "INSERT INTO tweets (username, timestamp, text, profilepicture) VALUES (%s, %s, %s, %s)",
                (username, timestamp, text, profile_picture_url)
            )
            conn.commit()
        except Exception as e:
            print(f"Error inserting tweet to DB: {e}")


while True:
    random.shuffle(usernames)  # Randomize the order of usernames
    for username in usernames:
        print(f"Last 10 tweets from {username}:")
        tweets, profile_picture_url = get_last_10_tweets(username)
        for tweet in tweets:
            print(f"- {tweet.text}")
            insert_tweet_to_db(username, tweet.created_at, tweet.text, profile_picture_url)
        time.sleep(180)  # Pause for 5 seconds between requests to avoid hitting rate limits

    print("Sleeping for 10 minutes...")
    time.sleep(700)  # Sleep for 10 minutes before running the process again

# Close the database connection when the script is terminated
cursor.close()
conn.close()