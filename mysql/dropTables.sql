DELIMITER //
  CREATE OR REPLACE PROCEDURE dropTables ()
  BEGIN
    DROP TABLE erc20logs;
    DROP TABLE erc20s;
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

