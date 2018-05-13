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
  , dbSelectTxFailed
  , dbInsertMsgCalls
  , dbSelectMsgCallsFroms
  , dbSelectMsgCallHasFrom
  , dbSelectMsgCallCountFrom
  , dbSelectMsgCallTxHashTo
  , dbInsertContractCreations
  , dbSelectContractCreationFroms
  , dbSelectContractCreationHasFrom
  , dbSelectContractCreationCountFrom
  , dbSelectContractCreationFromBlkNum
  , dbInsertInternalTxs
  , dbSelectInternalTxCountAddr
  , dbSelectInternalTxFromBlkNum
  , dbInsertDeadAccount
  , dbInsertDeadAccounts
  , dbSelectDeadAccountAddr
  , dbSelectDeadAccountAddrs
  , traceLogsMaskOp
  , hasOp
  , traceMaskOps
  , maskGetOps
  , dbCreateTableContractCode
  , dbSelectContractCodeLastBlkNum
  , dbInsertContractCode
  , dbSelectContractCodeErc20
  , dbSelectContractCodeAddrs
  , dbCreateTableErc20s
  , dbInsertErc20
  , dbSelectErc20LastBlkNum
  , dbSelectErc20Addrs
  , dbSelectErc20LogLastBlkNum
  , dbInsertErc20Log
  , dbInsertErc20Logs
  , spanMtxsByAddr
  ) where

import Control.DeepSeq (NFData(..),deepseq)
import Control.Monad (unless)
import Data.Bits
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.Char8 as LC8
import Data.Ethereum.EvmOp
import Data.Hashable
import qualified Data.HashMap.Lazy as HM
import Data.Maybe (fromJust)
import Data.List (sortBy)
import Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word (Word8,Word32,Word64)
import Database.MySQL.Base
import Network.Web3.Dapp.Bytes
import Network.Web3.Dapp.ERC20
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

-- | Interfaz para introducir/obtener datos de la BD MySQL
data MysqlTx =
    MyBlock BlockNum HexHash256 HexEthAddr Integer Gas
  | MyTx BlockNum Int HexHash256 Integer Gas Bool Word64
  | MyContractCreation BlockNum Int HexEthAddr HexEthAddr
  | MyMsgCall BlockNum Int HexEthAddr HexEthAddr
  | MyInternalTx BlockNum Int Word32 HexEthAddr HexEthAddr EvmOp
  | MyTouchedAccount BlockNum Int HexEthAddr
  | MyErc20Log BlockNum Int HexEthAddr Bool HexEthAddr HexEthAddr Uint256
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
  rnf (MyInternalTx blkNum txIdx idx fromA addr op) = blkNum
                                            `deepseq` txIdx
                                            `deepseq` idx
                                            `deepseq` fromA
                                            `deepseq` addr
                                            `deepseq` op
                                            `deepseq` ()
  rnf (MyTouchedAccount blkNum txIdx addr) = blkNum
                                   `deepseq` txIdx
                                   `deepseq` addr
                                   `deepseq` ()
  rnf (MyErc20Log blkNum txIdx addr transfer fromA toA amount) = blkNum
                                                       `deepseq` txIdx
                                                       `deepseq` addr
                                                       `deepseq` transfer
                                                       `deepseq` fromA
                                                       `deepseq` toA
                                                       `deepseq` amount
                                                       `deepseq` ()

instance NFData HexEthAddr where
  rnf (HexEthAddr addr) = addr `deepseq` ()

instance Hashable HexEthAddr

instance NFData EvmOp

mySetBlkNum = MySQLInt32U
mySetTxIdx = MySQLInt16U . fromIntegral
mySetIdx = MySQLInt32U
mySetGas = MySQLInt32U . fromIntegral
mySetGasLimit = MySQLInt64U
mySetOp = MySQLInt8U . toOpcode
mySetMop = MySQLBit
mySetBool = MySQLInt8U . fromIntegral . (fromEnum :: Bool -> Int)
mySetHexData = MySQLBytes . fromHex
mySetAddr = MySQLBytes . fromHex . getHexAddr
mySetUInt256 = MySQLBytes . bytesN
             . (fromUIntN :: Uint256 -> Bytes32)
mySetBigInt = mySetUInt256 . fromInteger
mySetText = MySQLText
mySetUnicode = MySQLBytes . TE.encodeUtf8
mySetUInt8 = MySQLInt8U . fromIntegral . (toInteger :: Uint8 -> Integer)

myGetOp = fromOpcode . myGetNum
myGetBool = (toEnum :: Int -> Bool) . myGetNum
myGetHexData (MySQLBytes bs) = toHex bs
myGetAddr = HexEthAddr . myGetHexData
myGetText (MySQLText t) = t
myGetUnicode (MySQLBytes bs) = TE.decodeUtf8 bs
myGetUInt8 (MySQLInt8U v) = (fromInteger :: Integer -> Uint8)
                          $ fromIntegral v
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

myReadAndSkipToEof :: IOS.InputStream a -> IO [a]
myReadAndSkipToEof s = IOS.toList s <* skipToEof s

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
  (MyInternalTx blkNum txIdx idx fromA addr op) ->
    insertInternalTxP blkNum txIdx idx fromA addr op
  (MyTouchedAccount blkNum txIdx addr) ->
    insertDeadAccountP blkNum txIdx addr
  (MyErc20Log blkNum txIdx addr transfer fromA toA amount) ->
    insertErc20LogP blkNum txIdx addr transfer fromA toA amount
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

readMaybeVal isValues f = do
  resp1 <- myReadAndSkipToEof isValues
  return $ case resp1 of
    [] -> Nothing
    [resp2] -> case resp2 of
      [] -> Nothing
      [val] -> Just (f val)

tableSelectLastBlkNumQ tblNom = Query $ "select blkNum from " <> tblNom <> " order by blkNum desc limit 1;"
tableSelectLastBlkNum myCon tblNom = do
  (colDefs,isValues) <- query_ myCon (tableSelectLastBlkNumQ tblNom)
  readMaybeVal isValues myGetNum

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
dbSelectLatestBlockNum myCon = tableSelectLastBlkNum myCon "blocks"

createTableTxsQ = "create table txs (blkNum integer unsigned not null, txIdx smallint unsigned not null, txHash binary(32) not null, txValue binary(32) not null, gas integer unsigned not null, failed boolean not null, maskOpcodes bit(64) not null, primary key (blkNum,txIdx), foreign key (blkNum) references blocks (blkNum));"
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
selectTxFailedQ = "select failed from txs where blkNum = ? and txIdx = ?;"
selectTxFailedP blkNum txIdx =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
dbSelectTxFailed myCon blkNum txIdx = do
  (colDefs,isValues) <- query myCon selectTxFailedQ (selectTxFailedP blkNum txIdx)
  fromJust <$> readMaybeVal isValues myGetBool

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
  if null addrs
    then return []
    else do
      (colDefs,isValues) <- query myCon (selectTableFromsQ tNom addrs)
                              (selectTableFromsP blkNum txIdx addrs)
      map (myGetAddr . head) <$> myReadAndSkipToEof isValues

createTableMsgCallsQ = "create table msgCalls (blkNum integer unsigned not null, txIdx smallint unsigned not null, fromA binary(20) not null, toA binary(20) not null, primary key (blkNum,txIdx), foreign key (blkNum,txIdx) references txs (blkNum,txIdx));"
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
  not . null <$> myReadAndSkipToEof isValues
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
  maybe 0 id <$> readMaybeVal isValues myGetNum
selectMsgCallTxHashToQ = "select t.txHash from msgCalls m join txs t on m.blkNum = t.blkNum and m.txIdx = t.txIdx where m.toA = ? order by t.blkNum,t.txIdx;"
selectMsgCallTxHashToP addr = [mySetAddr addr]
dbSelectMsgCallTxHashTo myCon addr = do
  (colDefs,isValues) <- query myCon selectMsgCallTxHashToQ
                          (selectMsgCallTxHashToP addr)
  map (myGetHexData . head) <$> myReadAndSkipToEof isValues

createTableContractCreationsQ = "create table contractCreations (blkNum integer unsigned not null, txIdx smallint unsigned not null, fromA binary(20) not null, contractA binary(20) not null, primary key (blkNum,txIdx), foreign key (blkNum,txIdx) references txs (blkNum,txIdx));"
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
  not . null <$> myReadAndSkipToEof isValues
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
  maybe 0 id <$> readMaybeVal isValues myGetNum
selectContractCreationFromBlkNumQ = "select blkNum,txIdx,fromA,contractA from contractCreations where blkNum >= ? and blkNum <= ?;"
selectContractCreationFromBlkNumP iniBlk lstBlk =
  [ mySetBlkNum iniBlk
  , mySetBlkNum lstBlk
  ]
dbSelectContractCreationFromBlkNum myCon iniBlk lstBlk = do
  (colDefs,isValues) <- query myCon selectContractCreationFromBlkNumQ
                          (selectContractCreationFromBlkNumP iniBlk lstBlk)
  map getMyContractCreation <$> myReadAndSkipToEof isValues

getMyContractCreation [vBlkNum,vTxIdx,vFromA,vContractA] =
  MyContractCreation (myGetNum vBlkNum) (myGetNum vTxIdx)
                     (myGetAddr vFromA) (myGetAddr vContractA)

createTableInternalTxsQ = "create table internalTxs (blkNum integer unsigned not null, txIdx smallint unsigned not null, idx mediumint unsigned not null, fromA binary(20) not null, addr binary(20) not null, opcode tinyint unsigned not null, primary key (blkNum,txIdx,idx), foreign key (blkNum,txIdx) references txs (blkNum,txIdx));"
createIndexInternalTxFromQ = "create index internalTxFrom on internalTxs (fromA);"
createIndexInternalTxAddrQ = "create index internalTxAddr on internalTxs (addr);"
insertInternalTxQ = "insert into internalTxs (blkNum,txIdx,idx,fromA,addr,opcode) value (?,?,?,?,?,?);"
insertInternalTxP blkNum txIdx idx fromA addr op =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetIdx idx
  , mySetAddr fromA
  , mySetAddr addr
  , mySetOp op
  ]
insertInternalTx myCon blkNum txIdx idx fromA addr op = do
  print ("itx", blkNum, txIdx, idx, fromA, addr, op)
  execute myCon insertInternalTxQ (insertInternalTxP blkNum txIdx idx fromA addr op) >>= printErr
dbInsertInternalTxs myCon = tableInsertMyTxs myCon "internalTxs" ["blkNum","txIdx","idx","fromA","addr","opcode"]
selectInternalTxCountAddrQ = "select count(*) from internalTxs where ((opcode = ? or opcode = ? or opcode = ? or opcode = ?) and fromA = ?) or (opcode = ? and addr = ?);"
selectInternalTxCountAddrP addr =
  [ mySetOp OpCALL
  , mySetOp OpCALLCODE
  , mySetOp OpDELEGATECALL
  , mySetOp OpCREATE
  , mySetAddr addr
  , mySetOp OpCREATE
  , mySetAddr addr
  ]
dbSelectInternalTxCountAddr myCon addr = do
  (colDefs,isValues) <- query myCon selectInternalTxCountAddrQ
                                (selectInternalTxCountAddrP addr)
  maybe 0 id <$> readMaybeVal isValues myGetNum

selectInternalTxFromBlkNumQ = "select blkNum,txIdx,idx,fromA,addr,opcode from internalTxs where blkNum >= ? and blkNum <= ? and opcode = ?;"
selectInternalTxFromBlkNumP iniBlk lstBlk opcode =
  [ mySetBlkNum iniBlk
  , mySetBlkNum lstBlk
  , mySetOp opcode
  ]
dbSelectInternalTxFromBlkNum myCon iniBlk lstBlk opcode = do
  (colDefs,isValues) <- query myCon selectInternalTxFromBlkNumQ
                                (selectInternalTxFromBlkNumP iniBlk lstBlk opcode)
  map getMyInternalTx <$> myReadAndSkipToEof isValues
  where
    getMyInternalTx [vBlkNum,vTxIdx,vIdx,vFromA,vAddr,vOpcode] =
      MyInternalTx (myGetNum vBlkNum) (myGetNum vTxIdx)
                   (myGetNum vIdx) (myGetAddr vFromA)
                   (myGetAddr vAddr) (myGetOp vOpcode)

createDeadAccountsQ = "create table deadAccounts (blkNum integer unsigned not null, txIdx smallint unsigned not null, addr binary(20) not null, primary key (addr), foreign key (blkNum,txIdx) references txs (blkNum,txIdx));"
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
  readMaybeVal isValues id
selectDeadAccountAddrsQ addrs =
  let addrsL = LBS.intercalate "," $ replicate (length addrs) "?"
  in Query $ "select addr from deadAccounts where addr in (" <> addrsL <> ");"
selectDeadAccountAddrsP = map mySetAddr
dbSelectDeadAccountAddrs myCon addrs = do
  if null addrs
    then return []
    else do
      (colDefs,isValues) <- query myCon (selectDeadAccountAddrsQ addrs)
                                       (selectDeadAccountAddrsP addrs)
      map (myGetAddr . head) <$> myReadAndSkipToEof isValues

dbInsertMyTx myCon mtx = case mtx of
  (MyBlock blkNum blkHash miner difficulty gasLimit) ->
    insertBlock myCon blkNum blkHash miner difficulty gasLimit
  (MyTx blkNum txIdx txHash value gas failed mop) ->
    insertTx myCon blkNum txIdx txHash value gas failed mop
  (MyContractCreation blkNum txIdx fromA contractA) ->
    insertContractCreation myCon blkNum txIdx fromA contractA
  (MyMsgCall blkNum txIdx fromA toA) ->
    insertMsgCall myCon blkNum txIdx fromA toA
  (MyInternalTx blkNum txIdx idx fromA addr op) ->
    insertInternalTx myCon blkNum txIdx idx fromA addr op
  _ -> error $ "dbInsertMyTx: " ++ show mtx

-- | Obtiene la mascara del trace de una transacción
traceLogsMaskOp :: [RpcTraceLog] -> Word64
traceLogsMaskOp = traceMaskOps . map (fromText . traceLogOp)

-- | Chequea la existencia de una operación en la mascara
hasOp mop op = (traceMaskOps [op] .&. mop) /= 0

-- | Obtiene la mascara de la lista de opcodes EVM
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

-- | Obtiene los opcodes EVM de la mascara
maskGetOps :: Word64 -> [[EvmOp]]
maskGetOps mop = map fst $ filter (\(_,m) -> (m .&. mop) /= 0) opcodeMap

-- | mapa de bits de los opcodes del EVM
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
  execute_ myCon "grant file on *.* to 'kitten'@'%';" >>= print
  execute_ myCon "flush privileges;" >>= print
  close myCon


createContractCodeQ = "create table contractsCode (blkNum integer unsigned not null, txIdx smallint unsigned not null, addr binary(20) not null, bzzr0 binary(32), code mediumblob not null, foreign key (blkNum,txIdx) references txs (blkNum,txIdx), primary key (blkNum,addr));"
createIndexBzzr0Q = "create index contractCodeBzzr0 on contractsCode (bzzr0);"
dbSelectContractCodeLastBlkNum myCon = tableSelectLastBlkNum myCon "contractsCode"
insertContractCodeQ = "insert into contractsCode (blkNum,txIdx,addr,bzzr0,code) value (?,?,?,?,?);"
insertContractCodeP blkNum txIdx addr mBzzr0 code =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr addr
  , maybe MySQLNull mySetHexData mBzzr0
  , mySetHexData code
  ]
dbInsertContractCode myCon blkNum txIdx addr mBzzr0 code = do
  print ("code", blkNum, txIdx, addr, mBzzr0, code)
  execute myCon insertContractCodeQ (insertContractCodeP blkNum txIdx addr mBzzr0 code) >>= printErr
selectContractCodeErc20Q blkIni blkFin =
  let sels = map (LC8.pack . T.unpack . stripHex . toHex)
           $ map LBS.fromStrict
           $ map erc20Selector $ HM.elems
           $ HM.filter erc20IsFunction
           $ HM.filter erc20IsRequired erc20Info
      selsQ = LBS.intercalate " and "
            $ map (\sel -> "hex(code) rlike '((?i)" <> sel <> ")'") sels
      blkIniQ = LC8.pack $ show blkIni
      blkFinQ = LC8.pack $ show blkFin
  in Query $ "select blkNum,txIdx,addr,bzzr0,code from contractsCode where blkNum >= " <> blkIniQ <> " and blkNum <= " <> blkFinQ <> " and " <> selsQ <> ";"
dbSelectContractCodeErc20 myCon blkIni blkFin = do
  (colDefs,isValues) <- query_ myCon (selectContractCodeErc20Q blkIni blkFin)
  readContractsCode isValues
selectContractCodeAddrsQ addrs =
  let addrsQ = LBS.intercalate "," $ replicate (length addrs) "?"
  in Query $ "select blkNum,txIdx,addr,bzzr0,code from contractsCode where addr in (" <> addrsQ <> ");"
selectContractCodeAddrsP = map mySetAddr
dbSelectContractCodeAddrs myCon addrs = do
  if null addrs
    then return []
    else do
      (colDefs,isValues) <- query myCon (selectContractCodeAddrsQ addrs)
                                       (selectContractCodeAddrsP addrs)
      filterLatestAddrs <$> readContractsCode isValues
  where
    filterLatestAddrs = map (last . sortByBlkNumTxIdx)
                      . HM.elems
                      . spanContractsCodeByAddr
    sortByBlkNumTxIdx = sortBy (\(b1,i1,_,_,_) (b2,i2,_,_,_) ->
      let c1 = compare b1 b2
          c2 = compare i1 i2
      in if c1==EQ then c2 else c1)
    spanContractsCodeByAddr = spanMtxsByAddr getContractCodeAddr
    getContractCodeAddr (_,_,addr,_,_) = addr
readContractsCode isValues =
  map getContractCode <$> myReadAndSkipToEof isValues
  where
    getContractCode [vBlkNum,vTxIdx,vAddr,vBzzr0,vCode] =
      ( myGetNum vBlkNum
      , myGetNum vTxIdx
      , myGetAddr vAddr
      , if vBzzr0==MySQLNull then Nothing else Just (myGetHexData vBzzr0)
      , myGetHexData vCode
      )

-- | Clasifica una lista de elementos por la dirección Ethereum
spanMtxsByAddr :: (a -> HexEthAddr) -> [a] -> HM.HashMap HexEthAddr [a]
spanMtxsByAddr getAddr = foldr spanLogByAddr HM.empty
  where
    spanLogByAddr log =
      HM.alter (Just . maybe [log] (log:)) (getAddr log)


dbCreateTableContractCode :: MySQLConn -> IO ()
dbCreateTableContractCode myCon = do
  execute_ myCon createContractCodeQ >>= printErr
  execute_ myCon createIndexBzzr0Q >>= printErr


createErc20sQ = "create table erc20s (blkNum integer unsigned not null, txIdx smallint unsigned not null, addr binary(20) not null, isErc20 boolean not null, name binary(255), symbol binary(32), decimals tinyint unsigned, foreign key (blkNum,txIdx) references txs (blkNum,txIdx), foreign key (blkNum,addr) references contractsCode (blkNum,addr), primary key (addr)) character set = utf8mb4, collate = utf8mb4_unicode_ci;"
insertErc20Q = "insert into erc20s (blkNum,txIdx,addr,isErc20,name,symbol,decimals) value (?,?,?,?,?,?,?);"
insertErc20P blkNum txIdx addr isErc20 mName mSymbol mDecimals =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr addr
  , mySetBool isErc20
  , maybe MySQLNull mySetUnicode mName
  , maybe MySQLNull mySetUnicode mSymbol
  , maybe MySQLNull mySetUInt8 mDecimals
  ]
dbInsertErc20 myCon blkNum txIdx addr isErc20 mName mSymbol mDecimals = do
  print ("erc20", blkNum, txIdx, addr, isErc20, mName, mSymbol, mDecimals)
  execute myCon insertErc20Q (insertErc20P blkNum txIdx addr isErc20 mName mSymbol mDecimals) >>= printErr
dbSelectErc20LastBlkNum myCon = tableSelectLastBlkNum myCon "erc20s"
selectErc20AddrsQ addrs =
  let addrsQ = LBS.intercalate "," $ replicate (length addrs) "?"
  in Query $ "select blkNum,txIdx,addr,isErc20,name,symbol,decimals from erc20s where addr in (" <> addrsQ <> ");"
selectErc20AddrsP addrs = map mySetAddr addrs
dbSelectErc20Addrs myCon addrs = do
  if null addrs
    then return []
    else do
      (colDefs,isValues) <- query myCon (selectErc20AddrsQ addrs)
                                       (selectErc20AddrsP addrs)
      map getMyErc20 <$> myReadAndSkipToEof isValues
  where
    getMyErc20 [vBlkNum,vTxIdx,vAddr,vIsErc20,vName,vSymbol,vDecimals] =
      ( myGetNum vBlkNum
      , myGetNum vTxIdx
      , myGetAddr vAddr
      , myGetBool vIsErc20
      , if vName==MySQLNull then Nothing else Just (myGetUnicode vName)
      , if vSymbol==MySQLNull then Nothing else Just (myGetUnicode vSymbol)
      , if vDecimals==MySQLNull then Nothing else Just (myGetUInt8 vDecimals)
      )

createErc20LogsQ = "create table erc20Logs (blkNum integer unsigned not null, txIdx smallint unsigned not null, addr binary(20) not null, transfer boolean not null, fromA binary(20) not null, toA binary(20) not null, amount binary(32) not null, foreign key (blkNum,txIdx) references txs (blkNum,txIdx), foreign key (addr) references erc20s (addr));"
createIndexErc20LogsAddrQ = "create index erc20LogsAddr on erc20Logs (addr);"
createIndexErc20LogsFromQ = "create index erc20LogsFrom on erc20Logs (fromA);"
createIndexErc20LogsToQ = "create index erc20LogsTo on erc20Logs (toA);"
createIndexErc20LogsAmountQ = "create index erc20LogsAmount on erc20Logs (amount);"
dbSelectErc20LogLastBlkNum myCon = tableSelectLastBlkNum myCon "erc20Logs"
insertErc20LogQ = "insert into erc20Logs (blkNum,txIdx,addr,transfer,fromA,toA,amount) value (?,?,?,?,?,?,?);"
insertErc20LogP blkNum txIdx addr transfer fromA toA amount =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr addr
  , mySetBool transfer
  , mySetAddr fromA
  , mySetAddr toA
  , mySetUInt256 amount
  ]
dbInsertErc20Log myCon blkNum txIdx addr transfer fromA toA amount = do
  print ("erc20log", blkNum, txIdx, addr, transfer, fromA, toA, amount)
  execute myCon insertErc20LogQ (insertErc20LogP blkNum txIdx addr transfer fromA toA amount) >>= printErr
dbInsertErc20Logs myCon = tableInsertMyTxs myCon "erc20Logs" ["blkNum","txIdx","addr","transfer","fromA","toA","amount"]

dbCreateTableErc20s :: MySQLConn -> IO ()
dbCreateTableErc20s myCon = do
  execute_ myCon createErc20sQ >>= printErr
  execute_ myCon createErc20LogsQ >>= printErr
  execute_ myCon createIndexErc20LogsAddrQ >>= printErr
  execute_ myCon createIndexErc20LogsFromQ >>= printErr
  execute_ myCon createIndexErc20LogsToQ >>= printErr
  execute_ myCon createIndexErc20LogsAmountQ >>= printErr

