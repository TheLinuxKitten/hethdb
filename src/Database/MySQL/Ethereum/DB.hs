{-# LANGUAGE OverloadedStrings #-}

--------------------------------------------------------------------------
--
-- Copyright: (c) Javier López Durá
-- License: BSD3
--
-- Maintainer: Javier López Durá <linux.kitten@gmail.com>
--
--------------------------------------------------------------------------

module Database.MySQL.Ethereum.DB
  ( MysqlTx(..)
  , defConInfo
  , dbCreateDB
  , dbCreateTables
  , dbInsertGenesis
  , dbInsertMyTx
  , dbSelectLatestBlockNum
  , dbInsertTxs
  , dbInsertMsgCalls
  , dbSelectMsgCallsFroms
  , dbSelectMsgCallHasFrom
  , dbSelectMsgCallCountFrom
  , dbInsertContractCreations
  , dbSelectContractCreationFroms
  , dbSelectContractCreationHasFrom
  , dbSelectContractCreationCountFrom
  , dbInsertInternalTxs
  , dbSelectInternalTxCountAddr
  , dbInsertDeadAccount
  , dbInsertDeadAccounts
  , dbSelectDeadAccountAddr
  , dbSelectDeadAccountAddrs
  , traceLogsMaskOp
  , hasOp
  , traceMaskOps
  , maskGetOps
  , dbCreateTableContractCode
  , dbInsertContractCode
  ) where

import Control.DeepSeq (NFData(..),deepseq)
import Control.Monad (unless)
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Ethereum.EvmOp
import Data.Maybe (fromJust)
import Data.Monoid ((<>))
import Data.Word (Word8,Word32,Word64)
import Database.MySQL.Base
import Network.Web3.Dapp.Bytes
import Network.Web3.Dapp.Int
import Network.Web3.HexText
import Network.Web3.Types
import System.IO (hPrint,stderr)
import qualified System.IO.Streams as IOS

defConInfo myUrl myPort = defaultConnectInfo
  { ciUser = "kitten"
  , ciPassword = "kitten"
  , ciDatabase = "ethdb"
  , ciHost = myUrl
  , ciPort = fromInteger myPort
  }

rootConInfo = defaultConnectInfo { ciUser = "root", ciPassword = "" }

data MysqlTx =
    MyBlock BlockNum HexHash256 HexEthAddr Integer Gas
  | MyTx BlockNum Int HexHash256 Integer Gas Bool Word64
  | MyContractCreation BlockNum Int HexEthAddr HexEthAddr
  | MyMsgCall BlockNum Int HexEthAddr HexEthAddr
  | MyInternalTx BlockNum Int Word32 HexEthAddr HexEthAddr Word8
  | MyTouchedAccount BlockNum Int HexEthAddr
  deriving (Eq,Show)

instance NFData MysqlTx where
  rnf (MyBlock blkNum blkHash miner difficulty gasLimit) = blkNum
                                                 `deepseq` blkHash
                                                 `deepseq` miner
                                                 `deepseq` difficulty
                                                 `deepseq` gasLimit
                                                 `deepseq` ()
  rnf (MyTx blkNum txIdx txHash value gas failed mop) = blkNum
                                              `deepseq` txIdx
                                              `deepseq` txHash
                                              `deepseq` value
                                              `deepseq` gas
                                              `deepseq` failed
                                              `deepseq` mop
                                              `deepseq` ()
  rnf (MyContractCreation blkNum txIdx fromA contractA) = blkNum
                                                `deepseq` txIdx
                                                `deepseq` fromA
                                                `deepseq` contractA
                                                `deepseq` ()
  rnf (MyMsgCall blkNum txIdx fromA toA) = blkNum
                                 `deepseq` txIdx
                                 `deepseq` fromA
                                 `deepseq` toA
                                 `deepseq` ()
  rnf (MyInternalTx blkNum txIdx idx fromA addr opcode) = blkNum
                                                `deepseq` txIdx
                                                `deepseq` idx
                                                `deepseq` fromA
                                                `deepseq` addr
                                                `deepseq` opcode
                                                `deepseq` ()
  rnf (MyTouchedAccount blkNum txIdx addr) = blkNum `deepseq` txIdx `deepseq` addr `deepseq` ()

instance NFData HexEthAddr where
  rnf (HexEthAddr addr) = addr `deepseq` ()

mySetBlkNum = MySQLInt32U
mySetTxIdx = MySQLInt16U . fromIntegral
mySetIdx = MySQLInt32U
mySetGas = MySQLInt32U . fromIntegral
mySetGasLimit = MySQLInt64U
mySetOp = MySQLInt8U
mySetMop = MySQLBit
mySetBool = MySQLInt8U . fromIntegral . (fromEnum :: Bool -> Int)
mySetHexData = MySQLBytes . fromHex
mySetAddr = MySQLBytes . fromHex . getHexAddr
mySetBigInt = MySQLBytes . bytesN
            . (fromUIntN :: Uint256 -> Bytes32)
            . fromInteger

myGetAddr (MySQLBytes bs) = HexEthAddr (toHex bs)
myGetNum myVal = case myVal of
  MySQLInt8U v -> fromIntegral v
  MySQLInt8 v -> fromIntegral v
  MySQLInt16U v -> fromIntegral v
  MySQLInt16 v -> fromIntegral v
  MySQLInt32U v -> fromIntegral v
  MySQLInt32 v -> fromIntegral v
  MySQLInt64U v -> fromIntegral v
  MySQLInt64 v -> fromIntegral v
  _ -> error (show myVal)

myReadAndSkipToEof :: IOS.InputStream a -> IO (Maybe a)
myReadAndSkipToEof s = IOS.read s <* skipToEof s

createTableGenesisQ = "create table genesis (addr binary(20) not null, balance binary(32) not null, primary key (addr));"
insertGenesisQ = "insert into genesis (addr,balance) value (?,?);"
insertGenesisP addr balance = [mySetAddr addr, mySetBigInt balance]
dbInsertGenesis myCon addr balance = do
  print ("gen",addr,toHex balance)
  execute myCon insertGenesisQ (insertGenesisP addr balance) >>= printErr

myTxValue mtx = case mtx of
  (MyTx blkNum txIdx txHash value gas failed mop) ->
    insertTxP blkNum txIdx txHash value gas failed mop
  (MyContractCreation blkNum txIdx fromA contractA) ->
    insertContractCreationP blkNum txIdx fromA contractA
  (MyMsgCall blkNum txIdx fromA toA) ->
    insertMsgCallP blkNum txIdx fromA toA
  (MyInternalTx blkNum txIdx idx fromA addr opcode) ->
    insertInternalTxP blkNum txIdx idx fromA addr opcode
  (MyTouchedAccount blkNum txIdx addr) -> insertDeadAccountP blkNum txIdx addr
  _ -> error $ "myTxValue: " ++ show mtx

tableInsertMyTxs myCon tblNom colNoms mtxs =
  unless (null mtxs) $ do
    let numCols = length colNoms
    let cols = LBS.intercalate "," colNoms
    let cols' = "(" <> cols <> ")"
    let valMask = LBS.intercalate "," $ replicate numCols "?"
    let valMask' = "(" <> valMask <> ")"
    let numVals = length mtxs
    let values = map myTxValue mtxs
    let vals = if numVals > 1
                then
                  let valMasks =  replicate numVals valMask'
                  in "values " <> LBS.intercalate "," valMasks
                else "value " <> valMask'
    let qry = "insert into " <> tblNom <> " " <> cols' <> " " <> vals <> ";"
    execute myCon (Query qry) (concat values) >>= printErr
    --print $ show qry ++ " <-- " ++ show values

createTableBlocksQ = "create table blocks (blkNum integer unsigned not null, blkHash binary(32) not null, miner binary(20) not null, difficulty binary(32) not null, gasLimit bigint unsigned not null, primary key (blkNum));"
createIndexMinerQ = "create index miner on blocks (miner);"
createIndexBlkHashQ = "create index blkHash on blocks (blkHash);"
insertBlockQ = "insert into blocks (blkNum,blkHash,miner,difficulty,gasLimit) value (?,?,?,?,?);"
insertBlockP blkNum blkHash miner difficulty gasLimit =
  [ mySetBlkNum blkNum
  , mySetHexData blkHash
  , mySetAddr miner
  , mySetBigInt difficulty
  , mySetGasLimit gasLimit
  ]
insertBlock myCon blkNum blkHash miner difficulty gasLimit = do
  print ("blk", blkNum, blkHash, miner, difficulty, gasLimit)
  execute myCon insertBlockQ (insertBlockP blkNum blkHash miner difficulty gasLimit) >>= printErr
insertBlocks myCon = tableInsertMyTxs myCon "blocks" ["blkNum","blkHash","miner","difficulty","gasLimit"]
selectLatestBlockQ = "select blkNum from blocks order by blkNum desc limit 1;"
dbSelectLatestBlockNum myCon = do
  (colDefs,isValues) <- query_ myCon selectLatestBlockQ
  resp <- fromJust <$> myReadAndSkipToEof isValues
  return $ case resp of
    [] -> Nothing
    [val] -> Just (myGetNum val)


createTableTxsQ = "create table txs (blkNum integer unsigned not null, txIdx smallint unsigned not null, txHash binary(32) not null, txValue binary(32) not null, gas integer unsigned not null, failed boolean not null, maskOpcodes bit(64) not null, primary key (blkNum,txIdx));"
createIndexTxHashQ = "create index txHash on txs (txHash);"
insertTxQ = "insert into txs (blkNum,txIdx,txHash,txValue,gas,failed,maskOpcodes) value (?,?,?,?,?,?,?);"
insertTxP blkNum txIdx txHash value gas failed mop =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetHexData txHash
  , mySetBigInt value
  , mySetGas gas
  , mySetBool failed
  , mySetMop mop
  ]
insertTx myCon blkNum txIdx txHash value gas failed mop = do
  print ("tx", blkNum, txIdx, txHash, value, gas, failed, toHex mop)
  execute myCon insertTxQ (insertTxP blkNum txIdx txHash value gas failed mop) >>= printErr
dbInsertTxs myCon = tableInsertMyTxs myCon "txs" ["blkNum","txIdx","txHash","txValue","gas","failed","maskOpcodes"]

selectTableFromsQ tNom addrs =
  let addrsL = LBS.intercalate "," $ replicate (length addrs) "?"
  in Query $ "select fromA from " <> tNom <> " where fromA in (" <> addrsL <> ") and (blkNum < ? or (blkNum = ? and txIdx < ?));"
selectTableFromsP blkNum txIdx addrs =
  map mySetAddr addrs ++
  [ mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
selectTableFroms tNom myCon blkNum txIdx addrs = do
  (colDefs,isValues) <- query myCon (selectTableFromsQ tNom addrs)
                          (selectTableFromsP blkNum txIdx addrs)
  maybe [] (map myGetAddr) <$> myReadAndSkipToEof isValues

createTableMsgCallsQ = "create table msgCalls (blkNum integer unsigned not null, txIdx smallint unsigned not null, fromA binary(20) not null, toA binary(20) not null, primary key (blkNum,txIdx));"
createIndexMsgCallFromQ = "create index msgCallFrom on msgCalls (fromA);"
createIndexMsgCallToQ = "create index msgCallTo on msgCalls (toA);"
insertMsgCallQ = "insert into msgCalls (blkNum,txIdx,fromA,toA) value (?,?,?,?);"
insertMsgCallP blkNum txIdx fromA toA =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr fromA
  , mySetAddr toA
  ]
insertMsgCall myCon blkNum txIdx fromA toA = do
  print ("call", blkNum, txIdx, fromA, toA)
  execute myCon insertMsgCallQ (insertMsgCallP blkNum txIdx fromA toA) >>= printErr
dbInsertMsgCalls myCon = tableInsertMyTxs myCon "msgCalls" ["blkNum","txIdx","fromA","toA"]
dbSelectMsgCallsFroms = selectTableFroms "msgCalls"
selectMsgCallHasFromQ = "select * from msgCalls where fromA = ? and (blkNum < ? or (blkNum = ? and txIdx < ?)) limit 1;"
selectMsgCallHasFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
dbSelectMsgCallHasFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectMsgCallHasFromQ
                          (selectMsgCallHasFromP blkNum txIdx addr)
  maybe False (not . null) <$> myReadAndSkipToEof isValues
selectMsgCallCountFromQ = "select count(*) from msgCalls where fromA = ? and (blkNum < ? or (blkNum = ? and txIdx < ?));"
selectMsgCallCountFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
dbSelectMsgCallCountFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectMsgCallCountFromQ
                          (selectMsgCallCountFromP blkNum txIdx addr)
  maybe 0 (myGetNum . head) <$> myReadAndSkipToEof isValues

createTableContractCreationsQ = "create table contractCreations (blkNum integer unsigned not null, txIdx smallint unsigned not null, fromA binary(20) not null, contractA binary(20) not null, primary key (blkNum,txIdx));"
createIndexContractCreationFromQ = "create index contractCreationFrom on contractCreations (fromA);"
createIndexContractCreationContractQ = "create index contractCreationContract on contractCreations (contractA);"
insertContractCreationQ = "insert into contractCreations (blkNum,txIdx,fromA,contractA) value (?,?,?,?);"
insertContractCreationP blkNum txIdx fromA contractA =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr fromA
  , mySetAddr contractA
  ]
insertContractCreation myCon blkNum txIdx fromA contractA = do
  print ("new", blkNum, txIdx, fromA, contractA)
  execute myCon insertContractCreationQ (insertContractCreationP blkNum txIdx fromA contractA) >>= printErr
dbInsertContractCreations myCon = tableInsertMyTxs myCon "contractCreations" ["blkNum","txIdx","fromA","contractA"]
dbSelectContractCreationFroms = selectTableFroms "contractCreations"
selectContractCreationHasFromQ = "select * from contractCreations where fromA = ? and (blkNum < ? or (blkNum = ? and txIdx < ?)) limit 1;"
selectContractCreationHasFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
dbSelectContractCreationHasFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectContractCreationHasFromQ
                          (selectContractCreationHasFromP blkNum txIdx addr)
  maybe False (not . null) <$> myReadAndSkipToEof isValues
selectContractCreationCountFromQ = "select count(*) from contractCreations where fromA = ? and (blkNum < ? or (blkNum = ? and txIdx < ?));"
selectContractCreationCountFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
dbSelectContractCreationCountFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectContractCreationCountFromQ
                          (selectContractCreationCountFromP blkNum txIdx addr)
  maybe 0 (myGetNum . head) <$> myReadAndSkipToEof isValues

createTableInternalTxsQ = "create table internalTxs (blkNum integer unsigned not null, txIdx smallint unsigned not null, idx mediumint unsigned not null, fromA binary(20) not null, addr binary(20) not null, opcode tinyint unsigned not null, primary key (blkNum,txIdx,idx));"
createIndexInternalTxFromQ = "create index internalTxFrom on internalTxs (fromA);"
createIndexInternalTxAddrQ = "create index internalTxAddr on internalTxs (addr);"
insertInternalTxQ = "insert into internalTxs (blkNum,txIdx,idx,fromA,addr,opcode) value (?,?,?,?,?,?);"
insertInternalTxP blkNum txIdx idx fromA addr opcode =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetIdx idx
  , mySetAddr fromA
  , mySetAddr addr
  , mySetOp opcode
  ]
insertInternalTx myCon blkNum txIdx idx fromA addr opcode = do
  print ("itx", blkNum, txIdx, idx, fromA, addr, toHex opcode)
  execute myCon insertInternalTxQ (insertInternalTxP blkNum txIdx idx fromA addr opcode) >>= printErr
dbInsertInternalTxs myCon = tableInsertMyTxs myCon "internalTxs" ["blkNum","txIdx","idx","fromA","addr","opcode"]
selectInternalTxCountAddrQ = "select count(*) from internalTxs where ((opcode = ? or opcode = ? or opcode = ? or opcode = ?) and fromA = ?) or (opcode = ? and addr = ?);"
selectInternalTxCountAddrP addr =
  [ mySetOp opCall
  , mySetOp opCallcode
  , mySetOp opDelegatecall
  , mySetOp opCreate
  , mySetAddr addr
  , mySetOp opCreate
  , mySetAddr addr
  ]
dbSelectInternalTxCountAddr myCon addr = do
  (colDefs,isValues) <- query myCon selectInternalTxCountAddrQ
                                (selectInternalTxCountAddrP addr)
  maybe 0 (myGetNum . head) <$> myReadAndSkipToEof isValues

opCreate = toOpcode OpCREATE
opCall = toOpcode OpCALL
opCallcode = toOpcode OpCALLCODE
opDelegatecall = toOpcode OpDELEGATECALL


createDeadAccountsQ = "create table deadAccounts (blkNum integer unsigned not null, txIdx smallint unsigned not null, addr binary(20) not null, primary key (addr));"
insertDeadAccountQ = "insert into deadAccounts (blkNum,txIdx,addr) value (?,?,?);"
insertDeadAccountP blkNum txIdx addr = 
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr addr
  ]
dbInsertDeadAccount myCon blkNum txIdx addr = do
  print ("dead",blkNum,txIdx,addr)
  execute myCon insertDeadAccountQ (insertDeadAccountP blkNum txIdx addr) >>= printErr
dbInsertDeadAccounts myCon = tableInsertMyTxs myCon "deadAccounts" ["blkNum","txIdx","addr"]
selectDeadAccountAddrQ = "select * from deadAccounts where addr = ?"
selectDeadAccountAddrP addr = [mySetAddr addr]
dbSelectDeadAccountAddr myCon addr = do
  (colDefs,isValues) <- query myCon selectDeadAccountAddrQ
                                      (selectDeadAccountAddrP addr)
  mVals <- myReadAndSkipToEof isValues
  return ((\vals -> if null vals then Nothing else Just (head vals)) <$> mVals)
selectDeadAccountAddrsQ addrs =
  let addrsL = LBS.intercalate "," $ replicate (length addrs) "?"
  in Query $ "select addr from deadAccounts where addr in (" <> addrsL <> ");"
selectDeadAccountAddrsP = map mySetAddr
dbSelectDeadAccountAddrs myCon addrs = do
  (colDefs,isValues) <- query myCon (selectDeadAccountAddrsQ addrs)
                                      (selectDeadAccountAddrsP addrs)
  maybe [] (map myGetAddr) <$> myReadAndSkipToEof isValues

dbInsertMyTx myCon mtx = case mtx of
  (MyBlock blkNum blkHash miner difficulty gasLimit) ->
    insertBlock myCon blkNum blkHash miner difficulty gasLimit
  (MyTx blkNum txIdx txHash value gas failed mop) ->
    insertTx myCon blkNum txIdx txHash value gas failed mop
  (MyContractCreation blkNum txIdx fromA contractA) ->
    insertContractCreation myCon blkNum txIdx fromA contractA
  (MyMsgCall blkNum txIdx fromA toA) ->
    insertMsgCall myCon blkNum txIdx fromA toA
  (MyInternalTx blkNum txIdx idx fromA addr opcode) ->
    insertInternalTx myCon blkNum txIdx idx fromA addr opcode
  _ -> error $ "dbInsertMyTx: " ++ show mtx

traceLogsMaskOp :: [RpcTraceLog] -> Word64
traceLogsMaskOp = traceMaskOps . map (fromText . traceLogOp)

hasOp mop op = (traceMaskOps [op] .&. mop) /= 0

traceMaskOps :: [EvmOp] -> Word64
traceMaskOps = fst . foldl traceMaskOp (0,opcodeMap)
  where
    traceMaskOp (r,opMap) op =
      let (opms,opMap') = partitionOpMap ([],[]) op opMap
          r' = if null opms then r else r .|. snd (head opms)
      in (r', opMap')
    partitionOpMap (opms1,opms2) _ [] = (reverse opms1, reverse opms2)
    partitionOpMap (opms1,opms2) op (opm:opms) =
      let (opms1',opms2') = if op `elem` fst opm
                              then (opm:opms1,opms2)
                              else (opms1,opm:opms2)
      in partitionOpMap (opms1',opms2') op opms

maskGetOps :: Word64 -> [[EvmOp]]
maskGetOps mop = map fst $ filter (\(_,m) -> (m .&. mop) /= 0) opcodeMap

opcodeMap = zip opcodes (map bit [0..63])
opcodes =
  [ [OpSTOP]
  , [OpADD, OpMUL, OpSUB, OpDIV, OpSDIV, OpMOD, OpSMOD, OpADDMOD, OpMULMOD, OpEXP, OpSIGNEXTEND]
  , [OpLT, OpGT, OpSLT, OpSGT, OpEQ, OpISZERO, OpAND, OpOR, OpXOR, OpNOT, OpBYTE]
  , [OpSHA3]
  , [OpADDRESS]
  , [OpBALANCE]
  , [OpORIGIN]
  , [OpCALLER]
  , [OpCALLVALUE]
  , [OpCALLDATALOAD, OpCALLDATASIZE, OpCALLDATACOPY]
  , [OpCODESIZE, OpCODECOPY]
  , [OpGASPRICE]
  , [OpEXTCODESIZE, OpEXTCODECOPY, OpRETURNDATASIZE, OpRETURNDATACOPY]
  , [OpBLOCKHASH, OpNUMBER]
  , [OpCOINBASE]
  , [OpTIMESTAMP]
  , [OpDIFFICULTY]
  , [OpGASLIMIT]
  , [OpPOP]
  , [OpMLOAD, OpMSTORE, OpMSTORE8]
  , [OpSLOAD]
  , [OpSSTORE]
  , [OpJUMP, OpJUMPI, OpJUMPDEST]
  , [OpPC, OpMSIZE, OpGAS]
  , mapOpIdx OpPUSH 1 32
  , mapOpIdx OpDUP 1 16
  , mapOpIdx OpSWAP 1 16
  , mapOpIdx OpLOG 0 4
  , [OpCREATE]
  , [OpCALL]
  , [OpCALLCODE]
  , [OpRETURN]
  , [OpDELEGATECALL]
  , [OpINVALID]
  , [OpSELFDESTRUCT]
  , [OpSTATICCALL]
  , [OpREVERT]
  ]
  where
    mapOpIdx op minIdx maxIdx = map op [minIdx..maxIdx]

printErr :: (Show a) => a -> IO ()
printErr = hPrint stderr

dbCreateTables :: MySQLConn -> IO ()
dbCreateTables myCon = do
  execute_ myCon createTableGenesisQ >>= printErr
  execute_ myCon createTableBlocksQ >>= printErr
  execute_ myCon createIndexMinerQ >>= printErr
  execute_ myCon createIndexBlkHashQ >>= printErr
  execute_ myCon createTableTxsQ >>= printErr
  execute_ myCon createIndexTxHashQ >>= printErr
  execute_ myCon createTableMsgCallsQ >>= printErr
  execute_ myCon createIndexMsgCallFromQ >>= printErr
  execute_ myCon createIndexMsgCallToQ >>= printErr
  execute_ myCon createTableContractCreationsQ >>= printErr
  execute_ myCon createIndexContractCreationFromQ >>= printErr
  execute_ myCon createIndexContractCreationContractQ >>= printErr
  execute_ myCon createTableInternalTxsQ >>= printErr
  execute_ myCon createIndexInternalTxFromQ >>= printErr
  execute_ myCon createIndexInternalTxAddrQ >>= printErr
  execute_ myCon createDeadAccountsQ >>= printErr

dbCreateDB :: IO ()
dbCreateDB = do
  (greet,myCon) <- connectDetail rootConInfo
  print greet
  --command myCon (COM_INIT_DB "ethdb") >>= putStrLn . show
  execute_ myCon "create database `ethdb`;" >>= print
  execute_ myCon "create user 'kitten' identified by 'kitten';" >>= print
  execute_ myCon "grant usage on *.* to 'kitten'@'%' identified by 'kitten';" >>= print
  execute_ myCon "grant all privileges on `ethdb`.* to 'kitten'@'%';" >>= print
  execute_ myCon "flush privileges;" >>= print
  close myCon

createContractCodeQ = "create table contractsCode (blkNum integer unsigned not null, addr binary(20) not null, isBinRuntime boolean not null, bzzr0 binary(32), code mediumblob not null, primary key (addr));"
createIndexBzzr0Q = "create index contractCodeBzzr0 on contractsCode (bzzr0);"
insertContractCodeQ = "insert into contractsCode (blkNum,addr,isBinRuntime,bzzr0,code) value (?,?,?,?,?);"
insertContractCodeP blkNum addr isBinRuntime mBzzr0 code =
  [ mySetBlkNum blkNum
  , mySetAddr addr
  , mySetBool isBinRuntime
  , maybe MySQLNull mySetHexData mBzzr0
  , mySetHexData code
  ]
dbInsertContractCode myCon blkNum addr isBinRuntime mBzzr0 code = do
  print ("code", blkNum, addr, isBinRuntime, mBzzr0, code)
  execute myCon insertContractCodeQ (insertContractCodeP blkNum addr isBinRuntime mBzzr0 code) >>= printErr

dbCreateTableContractCode :: MySQLConn -> IO ()
dbCreateTableContractCode myCon = do
  execute_ myCon createContractCodeQ >>= printErr
  execute_ myCon createIndexBzzr0Q >>= printErr

