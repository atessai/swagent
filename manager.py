import proxyseeker.proxyseeker as ps
from dbcaller.dbcaller import DBCall
import pandas as pd
from time import time, sleep

def clean(json_content):
    replays = []
    units = []
    raw_replays = json_content['data']['list']
    for replay in raw_replays:
        p1 = replay['playerOne']
        p2 = replay['playerTwo']
        replay = {
            # all info needed for the replays and players
            'replay_id':        replay['replayId'], 
            'date_created':     replay['createDate'],
            'player1':          p1['playerName'],
            'player2':          p2['playerName'],
            'winner':           replay['status'], 
            # all info needed for the players
            'player1_id':       p1['playerId'],
            'player2_id':       p2['playerId'],
            'player1_rank':     p1['playerScore'],
            'player2_rank':     p2['playerScore'],
            'player1_country':  p1['playerCountry'],
            'player2_country':  p2['playerCountry'],
            # all info needed for the drafts and units a bit
            'unit1':            p1['monsterInfoList'][0]['monsterId'],
            'unit2':            p1['monsterInfoList'][1]['monsterId'],
            'unit3':            p1['monsterInfoList'][2]['monsterId'],
            'unit4':            p1['monsterInfoList'][3]['monsterId'],
            'unit5':            p1['monsterInfoList'][4]['monsterId'],
            'unit6':            p2['monsterInfoList'][0]['monsterId'],
            'unit7':            p2['monsterInfoList'][1]['monsterId'],
            'unit8':            p2['monsterInfoList'][2]['monsterId'],
            'unit9':            p2['monsterInfoList'][3]['monsterId'],
            'unit10':           p2['monsterInfoList'][4]['monsterId'],
            'banned1_id':       p1['banMonsterId'],
            'banned2_id':       p2['banMonsterId'],
            'leader1_id':       p1['leaderMonsterId'],
            'leader2_id':       p2['leaderMonsterId']
        } 
        all_units = p1['monsterInfoList'] + p2['monsterInfoList'] 
        unit = {
            # all info needed for the units
            'id':           [ unit['monsterId'] for unit in all_units ],
            'filename':     [ unit['imageFilename'] for unit in all_units ],
            'name':         [ unit['monsterName'] for unit in all_units ],
            'element':      [ unit['element'] for unit in all_units ],
            'natstars':     [ unit['naturalStars'] for unit in all_units ],
        }
        replays.append(pd.DataFrame([replay]))
        units.append(pd.DataFrame(unit))
    return pd.concat(replays), pd.concat(units)

def run(laps=10, delay=60, timeout=300):
    print(f'starting to run {laps} iterations of the run routine to get replays...')
    times = []
    for lap in range(laps):
        print(f'Lap # {lap}')
        start = time()
        # manager sends a seeker to get response from request
        seeker = ps.Seeker()
        while not seeker.response:
            proxy_list = seeker.get_proxies()
            seeker.seek(proxy_list)
            if not seeker.response:
                if int(time() - start) == timeout:
                    print('Its been too long... its better to quit! BYE')
                    exit(0) 
                # update the user_agent just in case too 
                seeker.headers['User-Agent'] = seeker.getUserAgent(seeker.agent_filename)   
                print('No data found... retrying...')
        # request is then parsed and cleaned
        replays, units = clean(seeker.response.json())
        # once data is cleaned it is pipelined to the db and stored
        caller = DBCall()
        caller.load(replays, units)

        now = time() - start
        print(f'Time Elapsed: {now}\n')
        times.append(now)

        if laps - 1 > lap:
            sleep(delay)
        
    print(f'--- {laps} RUNS COMPLETED. TOTAL TIME: {round(sum(times)/60, 2)} mins | AVG TIME: {round(sum(times)/laps, 2)}')

if __name__ == '__main__':
    run(laps=150, delay=10)