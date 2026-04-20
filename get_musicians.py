import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from tqdm import tqdm
import pandas as pd
import string
import time
import sys


def create_spotify_client(client_id, client_secret):
    return spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
    )


def collect_per_query(sp, queries, per_query_limit=50, limit=10, sleep_time=0.1):
    artists_set = set()
    data = []

    for q in tqdm(queries, desc="Query loop"):
        local_count = 0
        offset = 0

        while local_count < per_query_limit:
            res = sp.search(
                q=q,
                type='artist',
                limit=limit,
                offset=offset
            )

            items = res['artists']['items']

            if not items:
                break

            for a in items:
                name = a['name']

                if name not in artists_set:
                    artists_set.add(name)

                    data.append({
                        "artist": name,
                        "query": q
                    })

                    local_count += 1

                    if local_count >= per_query_limit:
                        break

            offset += limit
            time.sleep(sleep_time)

    return data


def filter_artists_by_popularity(sp, df, min_popularity=30):
    results = []

    for name in tqdm(df['artist'], desc="Filtering artists"):
        try:
            res = sp.search(q=name, type='artist', limit=1)
            items = res['artists']['items']

            if not items:
                continue

            artist = items[0]
            popularity = artist['popularity']

            if popularity >= min_popularity:
                results.append({
                    "artist": artist['name'],
                    "popularity": popularity
                })

        except Exception:
            time.sleep(1)
            continue

        time.sleep(0.05)

    return pd.DataFrame(results)


def main():
    if len(sys.argv) < 3:
        raise ValueError("Usage: python script.py <client_id> <client_secret>")

    client_id = sys.argv[1]
    client_secret = sys.argv[2]

    sp = create_spotify_client(client_id, client_secret)

    alpha_queries = list(string.ascii_lowercase)

    artist_queries = [
        "Tupac Shakur", "The Notorious B.I.G.", "Kendrick Lamar", "Drake", "Mac Miller",
        "MF DOOM", "Freddie Gibbs", "Earl Sweatshirt", "Danny Brown",
        "The Beatles", "Radiohead", "Arctic Monkeys",
        "Pavement", "The Strokes", "Car Seat Headrest", "Black Midi",
        "King Gizzard & The Lizard Wizard",
        "Daft Punk", "Skrillex",
        "Aphex Twin", "Autechre", "Burial", "Four Tet", "Floating Points",
        "Johann Sebastian Bach", "Ludwig van Beethoven",
        "Philip Glass", "Steve Reich",
        "Miles Davis", "John Coltrane",
        "Kamasi Washington", "Shabaka Hutchings",
        "Taylor Swift", "Ariana Grande", "Lana del rey", "Justin Bieber",
        "Phoebe Bridgers", "Clairo", "Beabadoobee",
        "Stevie Wonder", "Beyoncé", "Frank Ocean", "Daniel Caesar", "SZA",
        "Solange", "Blood Orange", "Earth wind and Fire",
        "Bon Iver", "Sufjan Stevens", "Fleet Foxes", "Wet Leg",
        "Mount Eerie", "Big Thief", "Dijon",
        "Brian Eno", "Tim Hecker", "Oneohtrix Point Never",
        "Bad Bunny", "Shakira",
        "Natalia Lafourcade", "Rosalía",
        "BTS", "BLACKPINK", "THAMA", "Crush", "Beenzino",
        "HYUKOH", "Silica Gel", "ADOY", "Jannabi",
        "Utada Hikaru",
        "Cornelius", "Fishmans",
        "Fela Kuti", "Mdou Moctar", "Tinariwen",
        "Metallica",
        "Deafheaven", "Sun O)))", "Meshuggah",
        "Merzbow", "Death Grips"
    ]

    genre_queries = [
        "pop", "dance pop", "electropop", "teen pop",
        "hip hop", "rap", "trap", "drill", "boom bap",
        "conscious rap", "lofi hip hop", "underground hip hop",
        "rock", "alternative rock", "indie rock", "punk",
        "punk rock", "grunge", "hard rock",
        "metal", "heavy metal", "death metal", "black metal",
        "metalcore", "progressive metal",
        "indie", "indie pop", "indie folk", "bedroom pop",
        "lofi", "dream pop",
        "electronic", "edm", "house", "techno", "deep house",
        "trance", "dubstep", "ambient",
        "jazz", "smooth jazz", "bebop", "fusion jazz",
        "classical", "baroque", "romantic era", "orchestra",
        "piano music",
        "rnb", "soul", "neo soul", "funk",
        "latin", "reggaeton", "latin pop", "bachata",
        "salsa",
        "afrobeat", "afropop", "african music", "afro fusion", "afro house",
        "arabic music", "turkish pop", "indian music", "bollywood",
        "thai pop", "vietnamese pop",
        "latin trap",
        "brazilian music", "samba", "bossa nova",
        "cumbia", "tango",
        "kpop", "korean indie",
        "jpop", "japanese rock",
        "experimental", "noise music", "avant garde",
        "instrumental", "soundtrack", "film score"
    ]

    queries = list(set(alpha_queries + artist_queries + genre_queries))

    data = collect_per_query(
        sp,
        queries,
        per_query_limit=20,
        limit=10,
        sleep_time=0.05
    )

    df = pd.DataFrame(data)
    df.to_csv('music_artists_raw.csv', index=False, encoding='utf-8-sig')

    df_filtered = filter_artists_by_popularity(
        sp,
        df,
        min_popularity=40
    )

    df_filtered = (
        df_filtered
        .sort_values('popularity', ascending=False)
        .drop_duplicates('artist')
        .reset_index(drop=True)
    )

    df_filtered.to_csv('music_artists.csv', index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    main()