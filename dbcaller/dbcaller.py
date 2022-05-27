
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import pandas as pd
from auth import authentication

class DBCall:
    def __init__(self):
        """
            # The DBCALL Object will make certain SQL operations to the database
            # specified by the authentication. (authentication is moved to private
            # file after completion) Currently, it does not take any arguments but
            # it will use sys library to find the authentication file to log in with
            # those credentials.
        """
        self.tables = {} # allocation for db tables
        print('Connecting to DB ...')
        try:
            self.__db_connection = psycopg2.connect(
                host=authentication['ENDPOINT'], # uses aws rds
                port=authentication['PORTCODE'],
                user=authentication['USERNAME'], # got superuser grants
                password=authentication['PASSWORD'],
                database=authentication['DATABASE']
            )
        except psycopg2.OperationalError as err:
            print(f"Failed to connect to database [{authentication['DATABASE']}]\n{err}")
            exit(0)

    def loadDB(self, table_name):
        sql = f'''SELECT * FROM {table_name};'''
        with self.__db_connection.cursor() as cursor:
            try:
                cursor.execute(sql)
                # fetch results and place on df for easier manipulation
                col_names = [headers[0] for headers in cursor.description]
                return pd.DataFrame(cursor.fetchall(), columns=col_names)
            except Exception as err:
                print(err) # error will be only indicator of failure 
                return pd.DataFrame() # if err -> sends empty df

    def executeCommand(self, sql, values):
        with self.__db_connection.cursor() as cursor:
            try:
                # values are always as a list
                cursor.execute(sql, values)
                self.__db_connection.commit()
            except Exception as err:
                print(err) # on error should technically exit
                return False
        return True

    def update(self, table_name, variable_names, values, where):
        for variable, value in zip(variable_names, values):
            # does update of adding 1 to each variable in variable_names
            sql = f'''UPDATE {table_name} SET {variable} = {variable} + %s WHERE {where[0]} = %s;'''
            if not self.executeCommand(sql, [value, where[1]]):
                print('------update error----------------->')
                exit(0)

    def insert(self, table_name, variable_names, values):
        # insert all values that dont have NOT NULL rule on psql
        sql = f'''INSERT INTO {table_name} ({', '.join(variable_names)}) VALUES ({', '.join(['%s']*len(values))}) ON CONFLICT DO NOTHING;'''
        if not self.executeCommand(sql, values):
            print('------insert error----------------->')
            exit(0)
 
    def contains(self, table_name, identifier, identity_value):
        df = self.loadDB(table_name)
        if df.empty: # table hasnt been added with values or error on load...
            return False # if error on load the next submission to db will be caught anyways
        return identity_value in df[identifier].values

    def replay_catcher(self, replay_info):
        # player # 1 
        variable_names_players = ['id', 'nametag', 'rank', 'country']
        if not self.contains('players', 'id', replay_info['player1_id']):
            # create the player in players table
            self.insert(table_name='players', 
                        variable_names=variable_names_players, 
                        values=replay_info[['player1_id', 'player1', 'player1_rank', 'player1_country']])
            # create the player in player_counts table 
            self.insert(table_name='player_counts', 
                        variable_names=variable_names_players[:2], 
                        values=replay_info[['player1_id', 'player1']])
        # player # 2 
        if not self.contains('players', 'id', replay_info['player2_id']):
            # create the player in players table
            self.insert(table_name='players', 
                        variable_names=variable_names_players, 
                        values=replay_info[['player2_id', 'player2', 'player2_rank', 'player2_country']])
            # create the player in player_counts table 
            self.insert(table_name='player_counts', 
                        variable_names=variable_names_players[:2], 
                        values=replay_info[['player2_id', 'player2']])

        # drafts
        variable_names_drafts = ['unit1', 'unit2', 'unit3', 'unit4', 'unit5', 'banned', 'leader']
        d1 = list(replay_info[['unit1', 'unit2', 'unit3', 'unit4', 'unit5', 'banned1_id', 'leader1_id']].tolist())
        d2 = list(replay_info[['unit6', 'unit7', 'unit8', 'unit9', 'unit10', 'banned2_id', 'leader2_id']].tolist())

        df = self.loadDB('drafts')
        # check to see if draft 1 has been inputted into db before
        if df[ (df['unit1'] == d1[0]) & (df['unit2'] == d1[1]) & (df['unit1'] == d1[2]) & (df['unit2'] == d1[3]) &
                (df['unit1'] == d1[4]) & (df['unit2'] == d1[5]) & (df['unit1'] == d1[6])].empty:
            self.insert(table_name='drafts',
                        variable_names=variable_names_drafts,
                        values=d1)   
        # check to see if draft 2 has been inputted into db before
        if df[ (df['unit1'] == d2[0]) & (df['unit2'] == d2[1]) & (df['unit3'] == d2[2]) & (df['unit4'] == d2[3]) &
                (df['unit5'] == d2[4]) & (df['banned'] == d2[5]) & (df['leader'] == d2[6])].empty:
            self.insert(table_name='drafts',
                        variable_names=variable_names_drafts,
                        values=d2)

        # then you need to get their ids to add to the replay data and update the drafts counts
        df = self.loadDB('drafts')
        draft1_id = df[ (df['unit1'] == d1[0]) & (df['unit2'] == d1[1]) & (df['unit3'] == d1[2]) & (df['unit4'] == d1[3]) &
                        (df['unit5'] == d1[4]) & (df['banned'] == d1[5]) & (df['leader'] == d1[6])]['id'].to_list()[0] 
        draft2_id = df[ (df['unit1'] == d2[0]) & (df['unit2'] == d2[1]) & (df['unit3'] == d2[2]) & (df['unit4'] == d2[3]) &
                        (df['unit5'] == d2[4]) & (df['banned'] == d2[5]) & (df['leader'] == d2[6])]['id'].to_list()[0]

        # update counts for the drafts
        self.update(table_name='drafts', variable_names=['total_counts'], values=[1], where=['id', draft1_id])
        self.update(table_name='drafts', variable_names=['total_counts'], values=[1], where=['id', draft2_id])

        # update units
        for i, m in enumerate(d1):
            w = replay_info['winner'] # winner status
            b = replay_info['banned1_id'] # unit id that was banned
            l = replay_info['leader1_id'] # unnit id that was leader
            self.update(table_name='unit_counts', 
                        variable_names=['pick_count', 'win_count', 'first_pick_count', 'ban_count', 'leader_count'],
                        values=[1, 1 if w == 1 else 0, 1 if i == 0 else 0, 1 if m == b else 0, 1 if m == l else 0],
                        where=['id', m])
        for m in d2: # no enumerate bc it is not first_pick so we dont care. 
            w = replay_info['winner'] # winner status
            b = replay_info['banned2_id'] # unit id that was banned
            l = replay_info['leader2_id'] # unnit id that was leader
            self.update(table_name='unit_counts', 
                        variable_names=['pick_count', 'win_count', 'first_pick_count', 'ban_count', 'leader_count'],
                        values=[1, 1 if w == 2 else 0, 0, 1 if m == b else 0, 1 if m == l else 0],
                        where=['id', m])

        # update players
        self.update(table_name='player_counts', 
                    variable_names=['win_count', 'total_count'], 
                    values=[1 if replay_info['winner'] == 1 else 0, 1], 
                    where=['id', replay_info['player1_id']])
        self.update(table_name='player_counts',
                    variable_names=['win_count', 'total_count'],
                    values=[1 if replay_info['winner'] == 2 else 0, 1],
                    where=['id', replay_info['player2_id']])

        # inser the replay to db at last!
        variable_names_replays = ['id', 'date_created', 'player1', 'player2', 'draft1', 'draft2', 'winner']
        self.insert(table_name='replays', 
                    variable_names=variable_names_replays, 
                    values=[replay_info['replay_id'], 
                            replay_info['date_created'], 
                            replay_info['player1_id'], 
                            replay_info['player2_id'], 
                            draft1_id, # draft id is unique
                            draft2_id, # draft id is unique 
                            replay_info['winner']])

        print(f'replay {replay_info["replay_id"]} added to the DB !')

    def loadunits(self, unit_info, variable_names):
        # inserts new units to the database. This is threaded to make it a bit easier
        if not self.contains('units', 'id', unit_info[0]):
            print(f'::: {unit_info[2]} ::: not in db adding it now!')
            self.insert(table_name='units', variable_names=variable_names, values=unit_info)
            self.insert(table_name='unit_counts', variable_names=['id'], values=[unit_info[0]])
 
    def load(self, replays_df, units_df):
        # units
        variable_names = list(units_df.keys().tolist())
        values = units_df.drop_duplicates(subset=['id']).values
        with ThreadPoolExecutor(max_workers=50) as executor: # 50 workers for each unit in the unit_df
            executor.map(self.loadunits, values, [variable_names]*len(values))

        replays = []
        for _, replay_info in replays_df.iterrows():
            if not self.contains('replays', 'id', replay_info['replay_id']):
                replays.append(replay_info) # appending only the replays that have not been added to database

        # use threading to make slightly faster the job of updating and inserting into the db
        if replays:
            with ThreadPoolExecutor(max_workers=len(replays)) as executor: # max workers to the number of replays
                executor.map(self.replay_catcher, replays)