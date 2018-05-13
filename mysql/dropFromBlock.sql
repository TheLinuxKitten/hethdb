DELIMITER //
  CREATE OR REPLACE PROCEDURE dropFromBlock (IN n INTEGER UNSIGNED)
  BEGIN
    START TRANSACTION;
    DELETE FROM erc20logs where blkNum >= n;
    DELETE FROM erc20s where blkNum >= n;
    DELETE FROM contractsCode where blkNum >= n;
    DELETE FROM deadAccounts where blkNum >= n;
    DELETE FROM internalTxs where blkNum >= n;
    DELETE FROM msgCalls where blkNum >= n;
    DELETE FROM contractCreations WHERE blkNum >= n;
    DELETE FROM txs where blkNum >= n;
    DELETE FROM blocks WHERE blkNum >= n;
    COMMIT;
  END;
//
DELIMITER ;

