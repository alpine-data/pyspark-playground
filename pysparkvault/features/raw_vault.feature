Feature: Raw Vault Loading

    Background:
        Given we have date `t0`.
        And a date `t1` which is `1` day after `t0`.
        And a date `t2` which is `3` hours after `t1`.
        And a date `t3` which is `42` minutes after `t2`.
        And a date `t4` which is `6` hours after `t3`.
        And a date `t5` which is `1` day after `t4`.

        Given we have source database schema `imdb.schema.json` and a data vault mapping `imdb.mapping.yaml`

        Given we have CDC Batch `batch_1`.
        And the batch contains changes for table "movies".
            | OPERATION | LOAD_DATE | ID | NAME         | YEAR | DIRECTOR   | RATING  | RANK | LAST_UPDATE |
            | snapshot  | t1        | 1  | "Foo bar"    | 2004 | 1          | 9       | 3    | t0          |

        And the batch contains changes for table "other_table".
            | OPERATION | LOAD_DATE | ID | NAME         | YEAR | DIRECTOR   | RATING  | RANK | LAST_UPDATE |
            | snapshot  | t1        | 1  | "Foo bar"    | 2004 | 1          | 9       | 3    | t1          |

    Scenario: Simple Loading
        In Batch 1 actor "bla bla" is created, in a second it is modified and also deleted. In the third batch is re-created.
        We expect that the deletion is re-creation is correctly logged in the raw vault.
        
        When the CDC batch `b1` is loaded at `t2`.
        And the CDC batch `b2` is loaded at `t3`.

        Then we expect the raw vault table "HUB__MOVIES" to contain the following entries exactly once:
            | NAME          | YEAR | $__LOAD_DATE |
            | Foo bar       | 2004 | t2           |
            | Lorem Ipsum   | 2001 | t2           |
        And this is because
            """

            """

        And we expect the vault table "HUB__MOVIES" to contain exactly `2` entries.

        And the vault table "SAT__MOVIES" to contain the following entries exactly once.
            | LOAD_DATE | ID | DIRECTOR | RATING | RANK |
            | t1        | 1  | 1        | 9      | 3    |
            | t1        | 1  | 1        | 9      | 3    |
            | t1        | 1  | 1        | 9      | 3    |

        And the vault table should contain exactly `3` rows with the following attributes:
            Because ...
                
            | ID |
            | 1  |

        And this is because 
            """
            ashdb jsadksad 
            """

        And the vault table "SAT__EFFECTIVITY__MOVIES" to contain the following entries exactly once.
            | LOAD_DATE |

        When we receive a new CDC Batch 

