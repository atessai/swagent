DROP TABLE IF EXISTS replays;
DROP TABLE IF EXISTS drafts;
DROP TABLE IF EXISTS unit_counts;
DROP TABLE IF EXISTS units;
DROP TABLE IF EXISTS player_counts;
DROP TABLE IF EXISTS players;

--players
create table players (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    nametag VARCHAR(50) NOT NULL, 
    rank SMALLINT,
    country VARCHAR(4) NOT NULL,
    date_added DATE DEFAULT CURRENT_DATE
);

--player_counts
create table player_counts (
    id BIGSERIAL NOT NULL PRIMARY KEY REFERENCES players (id),
    nametag VARCHAR(50) NOT NULL,
    win_count INT DEFAULT 0,
    total_count INT DEFAULT 0
);

--units
create table units (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    filename VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(50) NOT NULL,
    element VARCHAR(10) NOT NULL,
    natstars SMALLINT NOT NULL
);

--unit_counts
create table unit_counts (
    id BIGSERIAL NOT NULL PRIMARY KEY REFERENCES units (id),
    pick_count INT DEFAULT 0,
    win_count INT DEFAULT 0,
    first_pick_count INT DEFAULT 0,
    ban_count INT DEFAULT 0,
    leader_count INT DEFAULT 0
);

--drafts
create table drafts (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    unit1 BIGSERIAL NOT NULL REFERENCES units (id),
    unit2 BIGSERIAL NOT NULL REFERENCES units (id), 
    unit3 BIGSERIAL NOT NULL REFERENCES units (id),
    unit4 BIGSERIAl NOT NULL REFERENCES units (id), 
    unit5 BIGSERIAL NOT NULL REFERENCES units (id),
    banned BIGSERIAL NOT NULL REFERENCES units (id), 
    leader BIGSERIAL NOT NULL REFERENCES units (id),
    total_counts INT DEFAULT 0
);

--replays
create table replays (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    date_created DATE NOT NULL,
    player1 BIGSERIAL NOT NULL REFERENCES players (id),
    player2 BIGSERIAL NOT NULL REFERENCES players (id),
    draft1 BIGSERIAL REFERENCES drafts (id),
    draft2 BIGSERIAL REFERENCES drafts (id),
    winner SMALLINT NOT NULL
);