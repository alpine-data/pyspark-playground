Feature: Raw Vault Loading

    Background:
        Given we have date `t0`.
        And a date `t1` which is `1` day after `t0`.
        And a date `t2` which is `3` hours after `t1`.
        And a date `t3` which is `42` minutes after `t2`.
        And a date `t4` which is `6` hours after `t3`.
        And a date `t5` which is `1` day after `t4`.
        And a date `t6` which is the maximum date.

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

        Given we have CDC batch `batch_2`.
        And the batch contains changes for table `movies`.
            | OPERATION     | LOAD_DATE | ID | NAME                         | YEAR | DIRECTOR   | RATING  | RANK | LAST_UPDATE |
            | create        | t1        | 5  | "Pulp Fiction"               | 1994 | 5          | 8.9     | 138  | t0          |
            | before_update | t1        | 4  | "Star Wars: Episode V"       | 1980 | 4          | 8.7     | 485  | t0          |
            | update        | t1        | 4  | "Star Wars: Episode V"       | 1980 | 4          | 8.4     | 344  | t0          |
            | before_update | t1        | 2  | "The Godfather"              | 1972 | 2          | 9.2     | 94   | t0          |
            | update        | t1        | 2  | "The Godfather"              | 1972 | 2          | 9.1     | 104  | t0          |
            | before_update | t2        | 2  | "The Godfather"              | 1972 | 2          | 9.1     | 104  | t0          |
            | update        | t2        | 2  | "The Godfather"              | 1972 | None       | 9.1     | 104  | t0          |
            | delete        | t3        | 2  | "The Godfather"              | 1972 | None       | 9.1     | 104  | t0          |
            | before_update | t1        | 1  | "The Shawshank Redemption"   | 1994 | 1          | 9.3     | 64   | t0          |
            | update        | t1        | 1  | "The Shawshank Redemption"   | 1994 | 1          | 9.6     | 5    | t0          |
            | before_update | t2        | 1  | "The Shawshank Redemption"   | 1994 | 1          | 9.6     | 5    | t0          |
            | update        | t2        | 1  | "The Shawshank Redemption"   | 1994 | None       | 9.6     | 5    | t0          |
            | before_update | t3        | 1  | "The Shawshank Redemption"   | 1994 | None       | 9.6     | 5    | t0          |
            | update        | t3        | 1  | "The Shawshank Redemption"   | 1994 | 2          | 9.6     | 5    | t0          |
            | before_update | t4        | 1  | "The Shawshank Redemption"   | 1994 | 2          | 9.6     | 5    | t0          |
            | update        | t4        | 1  | "The Shawshank Redemption"   | 1994 | 1          | 9.6     | 5    | t0          |
            | before_update | t1        | 3  | "The Dark Knight"            | 2008 | 3          | 9.0     | 104  | t0          |
            | update        | t1        | 3  | "The Dark Knight"            | 2008 | 3          | 9.3     | 45   | t0          |
            | delete        | t2        | 3  | "The Dark Knight"            | 2008 | 3          | 9.3     | 45   | t0          |
            | create        | t3        | 3  | "The Dark Knight"            | 2008 | 3          | 9.0     | 104  | t0          |
        And the batch contains changes for table `actors`.
            | OPERATION | LOAD_DATE | ID | NAME             | COUNTRY   |
            | create    | t1        | 9  | "John Travolta"  | "USA"     |
            | create    | t1        | 10 | "Liam Neeson"    | "USA"     |
        And the batch contains changes for table `directors`.
            | OPERATION | LOAD_DATE | ID | NAME                 | COUNTRY   |
            | create    | t1        | 5  | "Quentin Terintino"  | "USA"     |
        And the batch contains changes for table `castings`.
            | OPERATION | LOAD_DATE | MOVIE_ID  | ACTOR_ID  |
            | create    | t1        | 5         | 9         |
            | create    | t1        | 5         | 10        |

        Given we have CDC batch `batch_3`.
        And the batch contains changes for table `movies`.
            | OPERATION     | LOAD_DATE | ID | NAME             | YEAR | DIRECTOR   | RATING  | RANK | LAST_UPDATE |
            | delete        | t5        | 5  | "Pulp Fiction"   | 1994 | 5          | 8.9     | 138  | t0          |
        And the batch contains changes for table `actors`.
            | OPERATION | LOAD_DATE | ID | NAME             | COUNTRY   |
            | delete    | t5        | 9  | "John Travolta"  | "USA"     |
            | delete    | t5        | 10 | "Liam Neeson"    | "USA"     |
        And the batch contains changes for table `directors`.
            | OPERATION | LOAD_DATE | ID | NAME                 | COUNTRY   |
            | delete    | t5        | 5  | "Quentin Terintino"  | "USA"     |
        And the batch contains changes for table `castings`.
            | OPERATION | LOAD_DATE | MOVIE_ID  | ACTOR_ID  |
            | delete    | t5        | 5         | 9         |
            | delete    | t5        | 5         | 10        |

    Scenario: Simple raw vault loading in correct order
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_0`
            | NAME                          | YEAR | $__LOAD_DATE |
            | "Star Wars: Episode V"        | 1980 | t0           |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR | $__LOAD_DATE |
            | hk_0      | "Star Wars: Episode V"        | 1980 | t0           |

        And we expect the raw vault table `HUB__MOVIES` to contain exactly `5` entries.

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_0      | 4  | 4        | 8.7     | 485  | t0           |
            | hk_0      | 4  | 4        | 8.4     | 344  | t1           |

        And the raw vault table should contain exactly `2` rows with the following attributes:
            | $__HKEY |
            | hk_0    |

        And the raw vault table `SAT__EFFECTIVITY__MOVIES` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_0    | False         | t0             |
