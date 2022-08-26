import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import yaml
import pandas as pd
import pandas_gbq
import io

# get credentials
with open('config.yaml', 'r') as f:
    try:
        config = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        print(exc)

auth_manager = SpotifyClientCredentials(client_id=config['client_id'],
                                        client_secret=config['client_secret'])

BQ_TABLE = config['bq_table']
BQ_PROJECT = config['bq_project_id']


def get_info_from_track(record, type='track_name'):
    """
    Get data about tracks, artists and albums from a track request.

    :param record: actual response object containing all information.
    :param type: type of information requested. Could be any of ['track_name', 'duration',
    'album_name', 'artist_name', 'markets', 'total_tracks', 'release_date'].
    :return: Information string.
    """
    if type == 'track_name':
        return record['name']
    elif type == 'duration':
        return record['duration_ms']
    elif type == 'album_name':
        return record['album']['name']
    elif type == 'artist_name':
        return record['artists'][0]['name']
    elif type == 'markets':
        return record['available_markets']
    elif type == 'total_tracks':
        return record['album']['total_tracks']
    elif type == 'release_date':
        return record['album']['release_date']
    else:
        return 'Unable to get information'


def create_data_from_playlist(playlist_id, auth):
    """
    Generates a pandas dataframe from a spotify playlist.
    :param playlist_id: unique playlist id.
    :param auth: authorization manager.
    :return: Pandas DataFrame object
    """
    # create instance
    sp = spotipy.Spotify(auth_manager=auth)

    # get response object
    results = sp.playlist_items(playlist_id=playlist_id)

    # temporary dictionary for storing data
    temp_dict = {'track_name': [],
                 'popularity': [],
                 'duration': [],
                 'artists': [],
                 'release_date': [],
                 'album_name': [],
                 'album_type': [],
                 'total_album_tracks': []}

    # iterate over each track object
    for track in results['items']:
        try:
            # get track name
            track_name = track['track']['name']
            # get track popularity
            popularity = track['track']['popularity']
            # track duration
            duration = track['track']['duration_ms']
            # list of artist/artists
            artists = [records['name'] for records in track['track']['artists']]
            # release date
            release = track['track']['album']['release_date']
            # album name
            album_name = track['track']['album']['name']
            # album type
            album_type = track['track']['album']['album_type']
            # total tracks in album
            n_tracks = track['track']['album']['total_tracks']
        except:
            continue

        # append each item to dictionary values
        temp_dict['track_name'].append(track_name)
        temp_dict['popularity'].append(popularity)
        temp_dict['duration'].append(duration)
        temp_dict['artists'].append(artists)
        temp_dict['release_date'].append(release)
        temp_dict['album_name'].append(album_name)
        temp_dict['album_type'].append(album_type)
        temp_dict['total_album_tracks'].append(n_tracks)

    # create dataframe with dictionary
    df = pd.DataFrame(temp_dict)
    return df


def save_csv_to_disk(df, filename, dirname):
    """
    Saves a csv file to disk. Creates directory with 'dirname' if it doesn't exist.
    :param df: Dataframe
    :param filename: name of the saved file.
    :param dirname: directory where file will be stored.
    :return: None
    """
    path = os.path.join(dirname, filename)
    if os.path.exists(dirname):
        df.to_csv(path)
    else:
        os.makedirs('./' + dirname)
        df.to_csv(path)
        print(f'Directory {dirname} created and file saved to {path}.')


def get_playlist_ids(query, auth, lim=10):
    """
    Get list of playlist ids by passing in a search string.
    :param query: Search string or list of search stringsto provide. (eg: 'hindi' or ['hindi', 'spanish']
    :param auth: authorization manager containing credentials
    :param lim: result limit (default=10, max=50)
    :return: list of playlist ids.
    """
    list_of_items = []

    sp = spotipy.Spotify(auth_manager=auth)

    if type(query) == 'list':
        for string in query:
            response = sp.search(q=string, limit=lim, type='playlist')
            for item in response['playlists']['items']:
                list_of_items.append(item['id'])
    else:
        response = sp.search(q=query, limit=lim, type='playlist')
        for item in response['playlists']['items']:
            list_of_items.append(item['id'])
    return list_of_items


def bq_loader_function(df):
    """
    A simple helper function that converts a dataframe to a string and back, a workaround
    for the buggy pandas_gbq.to_gbq() method.
    :param df: Input dataframe
    :return: Polished dataframe
    """
    # temporarily store the dataframe as a csv in a string variable
    temp_csv_string = df.to_csv(sep=";", index=False)
    temp_csv_string_io = io.StringIO(temp_csv_string)
    # create new dataframe from string variable and return it
    new_df = pd.read_csv(temp_csv_string_io, sep=";")
    return new_df


if __name__ == '__main__':
    # get list of playlist ids
    playlist_ids = get_playlist_ids(['spanish'], auth=auth_manager, lim=50)

    # create empty dataframe
    df = pd.DataFrame(columns=['track_name',
                               'popularity',
                               'duration',
                               'artists',
                               'release_date',
                               'album_name',
                               'album_type',
                               'total_album_tracks'])

    # extract data from playlist and add to dataframe
    print('Collecting...')
    for p_id in playlist_ids:
        df = pd.concat([df, create_data_from_playlist(playlist_id=p_id, auth=auth_manager)])

    print(f'Data collected: {len(df)} rows, {len(df.columns)} columns.')

    # write to big query
    pandas_gbq.to_gbq(bq_loader_function(df),
                      destination_table=BQ_TABLE,
                      project_id=BQ_PROJECT,
                      if_exists='append')
