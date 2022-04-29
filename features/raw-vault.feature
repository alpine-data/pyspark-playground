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
            | OPERATION | LOAD_DATE | ID | NAME             | COUNTRY   | LAST_UPDATE |
            | snapshot  | t0        | 1  | "Tim Robbins"    | "USA"     | t0          |
            | snapshot  | t0        | 2  | "Morgan Freeman" | "USA"     | t0          |
            | snapshot  | t0        | 3  | "Bob Gunton"     | "USA"     | t0          |
            | snapshot  | t0        | 4  | "William Sadler" | "USA"     | t0          |
            | snapshot  | t0        | 5  | "Marlon Brando"  | "USA"     | t0          |
            | snapshot  | t0        | 6  | "Al Pacino"      | "USA"     | t0          |
            | snapshot  | t0        | 7  | "James Caan"     | "USA"     | t0          |
            | snapshot  | t0        | 8  | "Christian Bale" | "USA"     | t0          |
        And the batch contains changes for table `directors`.
            | OPERATION | LOAD_DATE | ID | NAME                     | COUNTRY   | LAST_UPDATE |
            | snapshot  | t0        | 1  | "Frank Darabont"         | "USA"     | t0          |
            | snapshot  | t0        | 2  | "Francis Ford Coppola"   | "USA"     | t0          |
            | snapshot  | t0        | 3  | "Christopher Nolan"      | "USA"     | t0          |
            | snapshot  | t0        | 4  | "Irvin Kershner"         | "USA"     | t0          |
        And the batch contains changes for table `castings`.
            | OPERATION | LOAD_DATE | MOVIE_ID  | ACTOR_ID  | LAST_UPDATE |
            | snapshot  | t0        | 1         | 1         | t0          |
            | snapshot  | t0        | 1         | 2         | t0          |
            | snapshot  | t0        | 2         | 3         | t0          |
            | snapshot  | t0        | 2         | 4         | t0          |
            | snapshot  | t0        | 3         | 5         | t0          |
            | snapshot  | t0        | 3         | 6         | t0          |
            | snapshot  | t0        | 4         | 7         | t0          |
            | snapshot  | t0        | 4         | 8         | t0          |

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
            | OPERATION | LOAD_DATE | ID | NAME             | COUNTRY   | LAST_UPDATE |
            | create    | t1        | 9  | "John Travolta"  | "USA"     | t0          |
            | create    | t1        | 10 | "Liam Neeson"    | "USA"     | t0          |
        And the batch contains changes for table `directors`.
            | OPERATION | LOAD_DATE | ID | NAME                 | COUNTRY   | LAST_UPDATE |
            | create    | t1        | 5  | "Quentin Terintino"  | "USA"     | t0          |
        And the batch contains changes for table `castings`.
            | OPERATION | LOAD_DATE | MOVIE_ID  | ACTOR_ID  | LAST_UPDATE |
            | create    | t1        | 5         | 9         | t0          |
            | create    | t1        | 5         | 10        | t0          |

        Given we have CDC batch `batch_3`.
        And the batch contains changes for table `movies`.
            | OPERATION     | LOAD_DATE | ID | NAME             | YEAR | DIRECTOR   | RATING  | RANK | LAST_UPDATE |
            | delete        | t5        | 5  | "Pulp Fiction"   | 1994 | 5          | 8.9     | 138  | t0          |
        And the batch contains changes for table `actors`.
            | OPERATION | LOAD_DATE | ID | NAME             | COUNTRY   | LAST_UPDATE |
            | delete    | t5        | 9  | "John Travolta"  | "USA"     | t0          |
            | delete    | t5        | 10 | "Liam Neeson"    | "USA"     | t0          |
        And the batch contains changes for table `directors`.
            | OPERATION | LOAD_DATE | ID | NAME                 | COUNTRY   | LAST_UPDATE |
            | delete    | t5        | 5  | "Quentin Terintino"  | "USA"     | t0          |
        And the batch contains changes for table `castings`.
            | OPERATION | LOAD_DATE | MOVIE_ID  | ACTOR_ID  | LAST_UPDATE |
            | delete    | t5        | 5         | 9         | t0          |
            | delete    | t5        | 5         | 10        | t0          |

    # Loads three CDC batches into the raw vault and checks if the raw vault tables exist.
    Scenario: Simple raw vault loading in correct order
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.

        Then the raw vault table `HUB__MOVIES` is created.
        And the raw vault table `HUB__ACTORS` is created.
        And the raw vault table `HUB__DIRECTORS` is created.
        And the raw vault table `LNK__MOVIES_DIRECTORS` is created.
        And the raw vault table `SAT__MOVIES` is created.
        And the raw vault table `SAT__ACTORS` is created.
        And the raw vault table `SAT__DIRECTORS` is created.
        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` is created.

        And we expect the raw vault table `HUB__MOVIES` to contain exactly `5` entries.
        And we expect the raw vault table `HUB__ACTORS` to contain exactly `10` entries.
        And we expect the raw vault table `HUB__DIRECTORS` to contain exactly `5` entries.
        And we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain exactly `6` entries.
        And we expect the raw vault table `SAT__MOVIES` to contain exactly `14` entries.
        And we expect the raw vault table `SAT__ACTORS` to contain exactly `10` entries.
        And we expect the raw vault table `SAT__DIRECTORS` to contain exactly `5` entries.
        And we expect the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain exactly `13` entries.

    # The movie "Star Wars: Episode V" is created in batch_1 and updated once in batch_2
    Scenario: Simple update in CDC batch without deletion
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_1`
            | NAME                          | YEAR |
            | "Star Wars: Episode V"        | 1980 |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_2`
            | ID    |
            | 4     |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_3`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_1          | hk_2              |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR |
            | hk_1      | "Star Wars: Episode V"        | 1980 |

        Then we expect the raw vault table `HUB__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_2      | 4     |
        
        Then we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_3      | hk_1          | hk_2              |

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_1      | 4  | 4        | 8.7     | 485  | t0           |
            | hk_1      | 4  | 4        | 8.4     | 344  | t1           |

        And the raw vault table `SAT__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME              | COUNTRY |
            | hk_2      | "Irvin Kershner"  | "USA"   |

        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_3    | False         | t0             |

    # The movie "Pulp Fiction" is created in batch_2 and deleted in batch_3
    Scenario: Simple delete in the CDC batch
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_1`
            | NAME                          | YEAR |
            | "Pulp Fiction"                | 1994 |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_2`
            | ID    |
            | 5     |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_3`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_1          | hk_2              |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR |
            | hk_1      | "Pulp Fiction"                | 1994 |

        Then we expect the raw vault table `HUB__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_2      | 5     |
        
        Then we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_3      | hk_1          | hk_2              |

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_1      | 5  | 5        | 8.9     | 138  | t1           |

        And the raw vault table `SAT__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME                  | COUNTRY |
            | hk_2      | "Quentin Terintino"   | "USA"   |

        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_3    | False         | t1             |
            | hk_3    | True          | t5             |

    # The movie "The Dark Knight" is created in batch_1, deleted and created again in batch_2
    Scenario: Update, delete and create in the CDC batch
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_1`
            | NAME                          | YEAR |
            | "The Dark Knight"             | 2008 |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_2`
            | ID    |
            | 3     |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_3`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_1          | hk_2              |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR |
            | hk_1      | "The Dark Knight"             | 2008 |

        Then we expect the raw vault table `HUB__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_2      | 3     |
        
        Then we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_3      | hk_1          | hk_2              |

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_1      | 3  | 3        | 9.0     | 104  | t0           |
            | hk_1      | 3  | 3        | 9.3     | 45   | t1           |
            | hk_1      | 3  | 3        | 9.0     | 104  | t3           |

        And the raw vault table `SAT__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME                  | COUNTRY |
            | hk_2      | "Christopher Nolan"   | "USA"   |

        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_3    | False         | t0             |
            | hk_3    | True          | t2             |
            | hk_3    | False         | t3             |

    # The director FK of the movie "The Godfather" is set to None in batch_2
    Scenario: Delete Link in CDC batch
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_1`
            | NAME                          | YEAR |
            | "The Godfather"               | 1972 |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_2`
            | ID    |
            | 2     |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_3`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_1          | hk_2              |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR |
            | hk_1      | "The Godfather"               | 1972 |

        Then we expect the raw vault table `HUB__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_2      | 2     |
        
        Then we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_3      | hk_1          | hk_2              |

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_1      | 2  | 2        | 9.2     | 94   | t0           |
            | hk_1      | 2  | 2        | 9.1     | 104  | t1           |
            | hk_1      | 2  | None     | 9.1     | 104  | t2           |

        And the raw vault table `SAT__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME                      | COUNTRY |
            | hk_2      | "Francis Ford Coppola"    | "USA"   |

        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_3    | False         | t0             |
            | hk_3    | True          | t2             |

    # The director FK of the movie "The Godfather" is set to None first and then set to other value in batch_2
    Scenario: Update and delete Link in CDC batch
        When the CDC batch `batch_1` is loaded at `t1`.
        And the CDC batch `batch_2` is loaded at `t2`.
        And the CDC batch `batch_3` is loaded at `t3`.
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_1`
            | NAME                          | YEAR |
            | "The Shawshank Redemption"    | 1994 |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_2`
            | ID    |
            | 1     |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_3`
            | ID    |
            | 2     |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_4`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_1          | hk_2              |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_5`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_1          | hk_3              |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR |
            | hk_1      | "The Shawshank Redemption"    | 1994 |

        Then we expect the raw vault table `HUB__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_2      | 1     |
            | hk_3      | 2     |
        
        Then we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_4      | hk_1          | hk_2              |
            | hk_5      | hk_1          | hk_3              |

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_1      | 1  | 1        | 9.3     | 64   | t0           |
            | hk_1      | 1  | 1        | 9.6     | 5    | t1           |
            | hk_1      | 1  | None     | 9.6     | 5    | t2           |
            | hk_1      | 1  | 2        | 9.6     | 5    | t3           |
            | hk_1      | 1  | 1        | 9.6     | 5    | t4           |

        And the raw vault table `SAT__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME                      | COUNTRY |
            | hk_2      | "Frank Darabont"          | "USA"   |
            | hk_3      | "Francis Ford Coppola"    | "USA"   |

        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_4    | False         | t0             |
            | hk_4    | True          | t2             |
            | hk_4    | False         | t4             |
            | hk_5    | False         | t3             |
            | hk_5    | True          | t4             |

    # Given the raw vault is already loaded
    Scenario: Test
        Given the raw vault is already loaded.
        When the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_0`
            | NAME                          | YEAR |
            | "The Shawshank Redemption"    | 1994 |
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_1`
            | NAME                          | YEAR |
            | "The Godfather"               | 1972 |
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_2`
            | NAME                          | YEAR |
            | "The Dark Knight"             | 2008 |
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_3`
            | NAME                          | YEAR |
            | "Star Wars: Episode V"        | 1980 |
        And the $__HKEY for the following line in the raw vault table `HUB__MOVIES` is assigned to `hk_4`
            | NAME                          | YEAR |
            | "Pulp Fiction"                | 1994 |
        And the $__HKEY for the following line in the raw vault table `HUB__ACTORS` is assigned to `hk_5`
            | ID    |
            | 1     |
        And the $__HKEY for the following line in the raw vault table `HUB__DIRECTORS` is assigned to `hk_6`
            | ID    |
            | 1     |
        And the $__HKEY for the following line in the raw vault table `LNK__MOVIES_DIRECTORS` is assigned to `hk_7`
            | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_0          | hk_6              |

        Then we expect the raw vault table `HUB__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | NAME                          | YEAR |
            | hk_0      | "The Shawshank Redemption"    | 1994 |
            | hk_1      | "The Godfather"               | 1972 |
            | hk_2      | "The Dark Knight"             | 2008 |
            | hk_3      | "Star Wars: Episode V"        | 1980 |
            | hk_4      | "Pulp Fiction"                | 1994 |
        And we expect the raw vault table `HUB__MOVIES` to contain exactly `5` entries.

        Then we expect the raw vault table `HUB__ACTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_5      | 1     |
        And we expect the raw vault table `HUB__ACTORS` to contain exactly `10` entries.

        Then we expect the raw vault table `HUB__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | ID    |
            | hk_6      | 1     |
        And we expect the raw vault table `HUB__DIRECTORS` to contain exactly `5` entries.

        Then we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | MOVIES__HKEY  | DIRECTORS__HKEY   |
            | hk_7      | hk_0          | hk_6              |
        And we expect the raw vault table `LNK__MOVIES_DIRECTORS` to contain exactly `6` entries.

        And the raw vault table `SAT__MOVIES` to contain the following entries exactly once:
            | $__HKEY   | ID | DIRECTOR | RATING  | RANK | $__LOAD_DATE |
            | hk_0      | 1  | 1        | 9.3     | 64   | t0           |
            | hk_0      | 1  | 1        | 9.6     | 5    | t1           |
            | hk_0      | 1  | None     | 9.6     | 5    | t2           |
            | hk_0      | 1  | 2        | 9.6     | 5    | t3           |
            | hk_0      | 1  | 1        | 9.6     | 5    | t4           |
            | hk_1      | 2  | 2        | 9.2     | 94   | t0           |
            | hk_1      | 2  | 2        | 9.1     | 104  | t1           |
            | hk_1      | 2  | None     | 9.1     | 104  | t2           |
            | hk_2      | 3  | 3        | 9.0     | 104  | t0           |
            | hk_2      | 3  | 3        | 9.3     | 45   | t1           |
            | hk_2      | 3  | 3        | 9.0     | 104  | t3           |
            | hk_3      | 4  | 4        | 8.7     | 485  | t0           |
            | hk_3      | 4  | 4        | 8.4     | 344  | t1           |
            | hk_4      | 5  | 5        | 8.9     | 138  | t1           |
        And we expect the raw vault table `SAT__MOVIES` to contain exactly `14` entries.

        And the raw vault table `SAT__ACTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME              | COUNTRY |
            | hk_5      | "Tim Robbins"     | "USA"   |
        And we expect the raw vault table `SAT__ACTORS` to contain exactly `10` entries.

        And the raw vault table `SAT__DIRECTORS` to contain the following entries exactly once:
            | $__HKEY   | NAME              | COUNTRY |
            | hk_6      | "Frank Darabont"  | "USA"   |
        And we expect the raw vault table `SAT__DIRECTORS` to contain exactly `5` entries.

        And the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_7    | False         | t0             |
            | hk_7    | True          | t2             |
            | hk_7    | False         | t4             |
        And we expect the raw vault table `SAT__EFFECTIVITY_MOVIES_DIRECTORS` to contain exactly `13` entries.

        And the raw vault table `SAT__EFFECTIVITY_MOVIES` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_0    | False         | t0             |
            | hk_1    | False         | t0             |
            | hk_1    | True          | t3             |
            | hk_2    | False         | t0             |
            | hk_2    | True          | t2             |
            | hk_2    | False         | t3             |
            | hk_3    | False         | t0             |
            | hk_4    | False         | t1             |
            | hk_4    | True          | t5             |
        And we expect the raw vault table `SAT__EFFECTIVITY_MOVIES` to contain exactly `9` entries.
            
        And the raw vault table `SAT__EFFECTIVITY_ACTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_5    | False         | t0             |
        And we expect the raw vault table `SAT__EFFECTIVITY_ACTORS` to contain exactly `10` entries.

        And the raw vault table `SAT__EFFECTIVITY_DIRECTORS` to contain the following entries exactly once:
            | $__HKEY | $__DELETED    | $__LOAD_DATE   |
            | hk_6    | False         | t0             |
        And we expect the raw vault table `SAT__EFFECTIVITY_DIRECTORS` to contain exactly `5` entries.
