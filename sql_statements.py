staging_events_table_create = ("""CREATE TABLE staging_events_table (
                num_songs           INTEGER              NULL,
                artist_id           varchar(MAX)         NOT NULL SORTKEY DISTKEY,
                artist_latitude     varchar(MAX)         NULL,
                artist_longitude    varchar(MAX)         NULL,
                artist_location     varchar(MAX)         NULL,
                artist_name         varchar(MAX)         NULL,
                song_id             varchar(MAX)         NOT NULL,
                title               varchar(MAX)         NULL,
                duration            DECIMAL(9)           NULL,
                year                INTEGER              NULL);
""")

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
