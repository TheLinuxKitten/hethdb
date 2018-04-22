DELIMITER //
  CREATE OR REPLACE PROCEDURE dropTables ()
  BEGIN
    DROP TABLE contractsCode;
    DROP TABLE deadAccounts;
    DROP TABLE internalTxs;
    DROP TABLE msgCalls;
    DROP TABLE contractCreations;
    DROP TABLE txs;
    DROP TABLE blocks;
    DROP TABLE genesis;
  END;
//
DELIMITER ;

