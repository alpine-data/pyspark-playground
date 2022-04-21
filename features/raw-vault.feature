Feature: Raw Vault Loading

    Background:
        Given we have date `t0`.
        And a date `t1` which is `1` day after `t0`.
        And a date `t2` which is `3` hours after `t1`.
        And a date `t3` which is `42` minutes after `t2`.
        And a date `t4` which is `6` hours after `t3`.
        And a date `t5` which is `1` day after `t4`.

        Given we have source database schema `imdb-schema.json` and a data vault mapping `imdb-mapping.yaml`.

        Given we have CDC batch `batch_1`.
        And the batch contains changes for table `movies`.
            | OPERATION | LOAD_DATE | ID | NAME                         | YEAR | DIRECTOR   | RATING  | RANK | LAST_UPDATE |
            | snapshot  | t0        | 1  | "The Shawshank Redemption"   | 1994 | 1          | 9.3     | 64   | t0          |
            | snapshot  | t0        | 2  | "The Godfather"              | 1972 | 2          | 9.2     | 94   | t0          |
            | snapshot  | t0        | 3  | "The Dark Knight"            | 2008 | 3          | 9.0     | 104  | t0          |
            | snapshot  | t0        | 4  | "Star Wars: Episode V"       | 1980 | 4          | 8.7     | 485  | t0          |
        And the batch contains changes for table `actors`.
            | OPERATION | LOAD_DATE | ID | NAME             | COUNTRY   |
            | snapshot  | t0        | 1  | "Tim Robbins"    | "USA"     |
            | snapshot  | t0        | 2  | "Morgan Freeman" | "USA"     |
            | snapshot  | t0        | 3  | "Bob Gunton"     | "USA"     |
            | snapshot  | t0        | 4  | "William Sadler" | "USA"     |
            | snapshot  | t0        | 5  | "Marlon Brando"  | "USA"     |
            | snapshot  | t0        | 6  | "Al Pacino"      | "USA"     |
            | snapshot  | t0        | 7  | "James Caan"     | "USA"     |
            | snapshot  | t0        | 8  | "Christian Bale" | "USA"     |
        And the batch contains changes for table `directors`.
            | OPERATION | LOAD_DATE | ID | NAME                     | COUNTRY   |
            | snapshot  | t0        | 1  | "Frank Darabont"         | "USA"     |
            | snapshot  | t0        | 2  | "Francis Ford Coppola"   | "USA"     |
            | snapshot  | t0        | 3  | "Christopher Nolan"      | "USA"     |
            | snapshot  | t0        | 4  | "Irvin Kershner"         | "USA"     |
        And the batch contains changes for table `castings`.
            | OPERATION | LOAD_DATE | MOVIE_ID  | ACTOR_ID  |
            | snapshot  | t0        | 1         | 1         |
            | snapshot  | t0        | 1         | 2         |
            | snapshot  | t0        | 2         | 3         |
            | snapshot  | t0        | 2         | 4         |
            | snapshot  | t0        | 3         | 5         |
            | snapshot  | t0        | 3         | 6         |
            | snapshot  | t0        | 4         | 7         |
            | snapshot  | t0        | 4         | 8         |

    Scenario: Simple raw vault loading in correct order
        In Batch 1 actor `bla bla` is created, in a second it is modified and also deleted. In the third batch is re-created.
        We expect that the deletion is re-creation is correctly logged in the raw vault.
        
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | NAME                          | YEAR | $__LOAD_DATE |
            | "The Shawshank Redemption"    | 1994 | t2           |
            | "The Godfather"               | 1972 | t2           |
            | "The Dark Knight"             | 2008 | t2           |
            | "Star Wars: Episode V"        | 1980 | t2           |

        And we expect the raw vault table `HUB__MOVIES` to contain exactly `2` entries.

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | NAME                          | YEAR | $__LOAD_DATE   |
            | "The Shawshank Redemption"    | 1994 | t0             |
            | "The Godfather"               | 1972 | t0             |
            | "The Dark Knight"             | 2008 | t0             |
            | "Star Wars: Episode V"        | 1980 | t0             |

        And the raw vault table should contain exactly `3` rows with the following attributes:
            | ID |
            | 1  |

        And the raw vault table `SAT__EFFECTIVITY__MOVIES` to contain the following entries exactly once:
            | $__DELETED    | $__LOAD_DATE   |
            | False         | t0             |
            | False         | t0             |
            | False         | t0             |
            | False         | t0             |
