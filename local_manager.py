import proxyseeker.proxyseeker as ps
from manager import clean
import pandas as pd
import numpy as np

def csv_retrieve(filename):
    return pd.read_csv(f'datasets/{filename}.csv')

def get_draft_id(df):
    if np.isnan(df['id'].max()):
        return 1
    return df['id'].max() + 1

def run():
    seeker = ps.Seeker()
    while True:
        if seeker.connect():
            # request is then parsed and cleaned
            replays, units = clean(seeker.response.json())

            # save units 
            units_df = csv_retrieve('units')
            new_units = pd.DataFrame(units.values, columns=['id', 'filename', 'name', 'element', 'natstars'])
            pd.concat([units_df, new_units]).drop_duplicates('id').to_csv('datasets/units.csv', index=False)

            # drop duplicate replays
            new_replays = []
            replay_df = csv_retrieve('replays')
            for _, replay_info in replays.iterrows():
                if not replay_info['replay_id'] in replay_df['id'].values: 
                    new_replays.append(replay_info) # appending only the replays that have not been added to database

            # save replays
            players_df = csv_retrieve('players')
            drafts_df = csv_retrieve('drafts')
            for new_replay in new_replays:
                # save player 1
                player1 = list(new_replay[['player1_id', 'player1', 'player1_rank', 'player1_country']].values) + [pd.to_datetime('today').strftime("%Y-%m-%d")]
                players_df = pd.concat([players_df, pd.DataFrame([player1], columns=['id', 'nametag', 'rank', 'country', 'date_added'])])
                # save player 2
                player2 = list(new_replay[['player2_id', 'player2', 'player2_rank', 'player2_country']].values) + [pd.to_datetime('today').strftime("%Y-%m-%d")]
                players_df = pd.concat([players_df, pd.DataFrame([player2], columns=['id', 'nametag', 'rank', 'country', 'date_added'])])

                variable_names = ['id', 'unit1', 'unit2', 'unit3', 'unit4', 'unit5', 'banned', 'leader']
                # save draft 1
                d1 = [get_draft_id(drafts_df)] + list(new_replay[['unit1', 'unit2', 'unit3', 'unit4', 'unit5', 'banned1_id', 'leader1_id']].values)
                drafts_df = pd.concat([drafts_df, pd.DataFrame([d1], columns=variable_names)])
                # save draft 2
                d2 = [get_draft_id(drafts_df)] + list(new_replay[['unit6', 'unit7', 'unit8', 'unit9', 'unit10', 'banned2_id', 'leader2_id']].values)
                drafts_df = pd.concat([drafts_df, pd.DataFrame([d2], columns=variable_names)])

                query = 'unit1 == {} and unit2 == {} and unit3 == {} and unit4 == {} and unit5 == {} and banned == {} and leader == {}'
                draft1_id = drafts_df.query(query.format(d1[1], d1[2], d1[3], d1[4], d1[5], d1[6], d1[7])).sort_values('id')['id'].values[0]
                draft2_id = drafts_df.query(query.format(d2[1], d2[2], d2[3], d2[4], d2[5], d2[6], d2[7])).sort_values('id')['id'].values[0]
                # save replays 
                variable_names_replays = ['id', 'date_created', 'player1', 'player2', 'draft1', 'draft2', 'winner']
                r = list(new_replay[['replay_id', 'date_created', 'player1_id', 'player2_id']].values) + [draft1_id, draft2_id] + [new_replay['winner']]
                replay_df = pd.concat([replay_df, pd.DataFrame([r], columns=variable_names_replays)])

            players_df['date_added'] = pd.to_datetime(players_df['date_added'])
            players_df.sort_values('date_added').drop_duplicates('id', keep='first') # keeps earliest record

            drafts_df = drafts_df.sort_values(['unit1', 'unit2', 'unit3', 'unit4', 'unit5', 'banned', 'leader'])
            drafts_df = drafts_df.drop_duplicates(['unit1', 'unit2', 'unit3', 'unit4', 'unit5', 'banned', 'leader'], keep='first')

            players_df.to_csv('datasets/players.csv', index=False)
            drafts_df.to_csv('datasets/drafts.csv', index=False)
            replay_df.to_csv('datasets/replays.csv', index=False)

        else:
            exit(0)
        break


if __name__ == '__main__':
    run()