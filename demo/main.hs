{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wmissing-signatures #-}

--------------------------------------------------------------------------
--
-- Copyright: (c) Javier López Durá
-- License: BSD3
--
-- Maintainer: Javier López Durá <linux.kitten@gmail.com>
--
--------------------------------------------------------------------------

module Main where

import Control.Concurrent (threadDelay)
import Control.DeepSeq (NFData(..),deepseq,force)
import Control.Monad (filterM,unless,when)
import Control.Monad.IO.Class
import Control.Parallel.Strategies (parList,rdeepseq,withStrategy)
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Ethereum.EvmOp
import Data.Maybe (fromJust, isJust)
import Data.List (nub)
import Data.Monoid ((<>))
import Data.Ethereum.EvmDisasm
import qualified Data.Ethereum.RLP as RLP
import qualified Data.Text as T
import Data.Tree
import Data.Word (Word8,Word32,Word64)
import Database.MySQL.Base
import Network.JsonRpcConn (LogLevel(..), filterLoggerLogLevel)
import Network.Web3
import Network.Web3.Dapp.Bytes
import Network.Web3.Dapp.EthABI (keccak256)
import Network.Web3.Dapp.Int
import System.Environment (getArgs,getProgName)
import System.IO (BufferMode(..),hPrint,hPutStrLn,hSetBuffering,stderr,stdout)
import qualified System.IO.Streams as IOS
import System.Posix.Signals

getOps :: IO (String,Integer,String,Maybe BlockNum,BlockNum,Bool,Bool,Bool,Bool)
getOps = do
  prog <- getProgName
  go prog ("localhost",3306,"http://localhost:8545",Nothing,100,False,False,False,False) <$> getArgs
  where
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--myHttp":a:as) = go p (a,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--myPort":a:as) = go p (myUrl,read a,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--ethHttp":a:as) = go p (myUrl,myPort,a,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--iniBlk":a:as) = go p (myUrl,myPort,ethUrl,Just $ read a, numBlks, iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--numBlks":a:as) = go p (myUrl,myPort,ethUrl,mIniBlk,read a,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--initDb":as) = go p (myUrl,myPort,ethUrl,mIniBlk,numBlks, True,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--par":as) = go p (myUrl,myPort,ethUrl,mIniBlk,numBlks, iniDb,True,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--log":as) = go p (myUrl,myPort,ethUrl,mIniBlk,numBlks, iniDb,doPar,True,doTest) as
    go p (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--test":as) = go p (myUrl,myPort,ethUrl,mIniBlk,numBlks, iniDb,doPar,doLog,True) as
    go p _ ("-h":as) = msgUso p
    go p _ ("--help":as) = msgUso p
    go _ r [] = r
    msgUso p = error $ p ++ " [-h|--help] [--myHttp myUrl] [--myPort <port>] [--ethHttp ethUrl] [--iniBlk <num>] [--numBlks <num>] [--initDb] [--par] [--log] [--test]"

defConInfo myUrl myPort = defaultConnectInfo
  { ciUser = "kitten"
  , ciPassword = "kitten"
  , ciDatabase = "ethdb"
  , ciHost = myUrl
  , ciPort = fromInteger myPort
  }

rootConInfo = defaultConnectInfo { ciUser = "root", ciPassword = "" }

putStrLnErr = hPutStrLn stderr

printErr :: (Show a) => a -> IO ()
printErr = hPrint stderr

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
mySetHash = MySQLBytes . fromHex
mySetAddr = MySQLBytes . fromHex . getHexAddr
mySetBigInt = MySQLBytes . bytesN
            . (fromUIntN :: Uint256 -> Bytes32)
            . fromInteger

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
insertGenesis myCon addr balance = do
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
  , mySetHash blkHash
  , mySetAddr miner
  , mySetBigInt difficulty
  , mySetGasLimit gasLimit
  ]
insertBlock myCon blkNum blkHash miner difficulty gasLimit = do
  print ("blk", blkNum, blkHash, miner, difficulty, gasLimit)
  execute myCon insertBlockQ (insertBlockP blkNum blkHash miner difficulty gasLimit) >>= printErr
insertBlocks myCon = tableInsertMyTxs myCon "blocks" ["blkNum","blkHash","miner","difficulty","gasLimit"]
selectLatestBlockQ = "select blkNum from blocks order by blkNum desc limit 1;"
selectLatestBlock myCon = do
  (colDefs,isValues) <- query_ myCon selectLatestBlockQ
  myGetNum . head . fromJust <$> myReadAndSkipToEof isValues


createTableTxsQ = "create table txs (blkNum integer unsigned not null, txIdx smallint unsigned not null, txHash binary(32) not null, txValue binary(32) not null, gas integer unsigned not null, failed boolean not null, maskOpcodes bit(64) not null, primary key (blkNum,txIdx));"
createIndexTxHashQ = "create index txHash on txs (txHash);"
insertTxQ = "insert into txs (blkNum,txIdx,txHash,txValue,gas,failed,maskOpcodes) value (?,?,?,?,?,?,?);"
insertTxP blkNum txIdx txHash value gas failed mop =
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetHash txHash
  , mySetBigInt value
  , mySetGas gas
  , mySetBool failed
  , mySetMop mop
  ]
insertTx myCon blkNum txIdx txHash value gas failed mop = do
  print ("tx", blkNum, txIdx, txHash, value, gas, failed, toHex mop)
  execute myCon insertTxQ (insertTxP blkNum txIdx txHash value gas failed mop) >>= printErr
insertTxs myCon = tableInsertMyTxs myCon "txs" ["blkNum","txIdx","txHash","txValue","gas","failed","maskOpcodes"]

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
insertMsgCalls myCon = tableInsertMyTxs myCon "msgCalls" ["blkNum","txIdx","fromA","toA"]
selectMsgCallHasFromQ = "select * from msgCalls where fromA = ? where blkNum < ? or (blkNum = ? and txIdx < ?) limit 1;"
selectMsgCallHasFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
selectMsgCallHasFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectMsgCallHasFromQ
                          (selectMsgCallHasFromP blkNum txIdx addr)
  not . null . fromJust <$> myReadAndSkipToEof isValues
selectMsgCallCountFromQ = "select count(*) from msgCalls where fromA = ? where blkNum < ? or (blkNum = ? and txIdx < ?);"
selectMsgCallCountFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
selectMsgCallCountFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectMsgCallCountFromQ
                          (selectMsgCallCountFromP blkNum txIdx addr)
  myGetNum . head . fromJust <$> myReadAndSkipToEof isValues

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
insertContractCreations myCon = tableInsertMyTxs myCon "contractCreations" ["blkNum","txIdx","fromA","contractA"]
selectContractCreationHasFromQ = "select * from contractCreations where fromA = ? where blkNum < ? or (blkNum = ? and txIdx < ?) limit 1;"
selectContractCreationHasFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
selectContractCreationHasFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectContractCreationHasFromQ
                          (selectContractCreationHasFromP blkNum txIdx addr)
  not . null . fromJust <$> myReadAndSkipToEof isValues
selectContractCreationCountFromQ = "select count(*) from contractCreations where fromA = ? where blkNum < ? or (blkNum = ? and txIdx < ?);"
selectContractCreationCountFromP blkNum txIdx addr =
  [ mySetAddr addr
  , mySetBlkNum blkNum
  , mySetBlkNum blkNum
  , mySetTxIdx txIdx
  ]
selectContractCreationCountFrom myCon blkNum txIdx addr = do
  (colDefs,isValues) <- query myCon selectContractCreationCountFromQ
                          (selectContractCreationCountFromP blkNum txIdx addr)
  myGetNum . head . fromJust <$> myReadAndSkipToEof isValues

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
insertInternalTxs myCon = tableInsertMyTxs myCon "internalTxs" ["blkNum","txIdx","idx","fromA","addr","opcode"]
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
selectInternalTxCountAddr myCon addr = do
  (colDefs,isValues) <- query myCon selectInternalTxCountAddrQ
                                (selectInternalTxCountAddrP addr)
  myGetNum . head . fromJust <$> myReadAndSkipToEof isValues


createDeadAccountsQ = "create table deadAccounts (blkNum integer unsigned not null, txIdx smallint unsigned not null, addr binary(20) not null, primary key (addr));"
insertDeadAccountQ = "insert into deadAccounts (blkNum,txIdx,addr) value (?,?,?);"
insertDeadAccountP blkNum txIdx addr = 
  [ mySetBlkNum blkNum
  , mySetTxIdx txIdx
  , mySetAddr addr
  ]
insertDeadAccount myCon blkNum txIdx addr = do
  print ("dead",blkNum,txIdx,addr)
  execute myCon insertDeadAccountQ (insertDeadAccountP blkNum txIdx addr) >>= printErr

selectDeadAccountAddrQ = "select * from deadAccounts where addr = ?"
selectDeadAccountAddrP addr = [mySetAddr addr]
selectDeadAccountAddr myCon addr = do
  (colDefs,isValues) <- query myCon selectDeadAccountAddrQ
                                      (selectDeadAccountAddrP addr)
  mVals <- myReadAndSkipToEof isValues
  return ((\vals -> if null vals then Nothing else Just (head vals)) <$> mVals)

runWeb3 doLog ethUrl f = runWeb3N 1
  where
    runWeb3N n = do
      er <- runWeb3'
      case er of
        Left e ->
          if n > 10
            then return er
            else do
              threadDelay (5*10^6)
              runWeb3N (n+1)
        Right r -> return er
    runWeb3' = runStderrLoggingT
             $ filterLoggerLogLevel
                   (if doLog then LevelDebug else LevelOther "NoLogs")
             $ runWeb3HttpT 15 15 ethUrl f

main :: IO ()
main = do
  (myUrl,myPort,ethUrl,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) <- getOps
  updateDb doTest myUrl myPort ethUrl doPar mIniBlk numBlks iniDb
  {-if doTest
    then tests myUrl myPort ethUrl iniBlk numBlks doLog
    else updateDb doTest myUrl myPort ethUrl doPar iniBlk numBlks iniDb-}

tests myUrl myPort ethUrl iniBlk numBlks doLog = do
  testLongTrace ethUrl
  {-
  testLongQ myUrl myPort
  let addr = HexEthAddr "0x6a0a0fc761c612c340a0e98d33b37a75e5268472"
  let nonces = [1..9999]
  let cAddrs = map (contractAddress addr) nonces
  mapM_ print $ zip cAddrs nonces
  mapM_ (putStrLn . T.unpack) $ fromRight "parseEvmCode" $ parseEvmCode "0x6004600c60003960046000f3600035ff00000000000000000000000000000000"
  code <- fromRight "eth_getCode"
      <$> runWeb3 False ethUrl (eth_getCode (HexEthAddr "0x6a0a0fc761c612c340a0e98d33b37a75e5268472") RPBLatest)
  mapM_ (putStrLn . T.unpack) $ fromRight "parseEvmCode" $ parseEvmCode code
  testTraceTree doLog ethUrl iniBlk numBlks
  -}

testLongQ myUrl myPort = do
  (greet,myCon) <- connectDetail (defConInfo myUrl myPort)
  printErr greet
  (colDefs,iMyValues) <- query_ myCon "select * from blocks;"
  print colDefs
  mVals <- mapM (\_ -> IOS.read iMyValues) (replicate 30 "a")
  mapM_ print mVals
  skipToEof iMyValues
  close myCon

testLongTrace ethUrl = do
  --traceTx <- getTraceTx ethUrl "0x79f03e00803211ae61f7a63c6840aafd3d011252a46dfeb56293e923bca0140d"
  traceTx <- getTraceTx ethUrl "0xa5080c8060cebd905fd9eac05ddd514a9a7904e770d75f85c5b03c2e1f88ac02"
  let rLogs = snd $ reduceTraceLogs $ force $ traceValueTxLogs traceTx
  print rLogs
  print $ traceTxTree rLogs
  --print traceTx

contractAddress :: HexEthAddr -> Integer -> HexEthAddr
contractAddress addr nonce =
  let addrRlp = RLP.rlpEncode
              $ (fromHex :: T.Text -> BS.ByteString)
              $ getHexAddr addr
      nonceRlp = RLP.rlpEncode (nonce - 1)
      bs = RLP.rlpBs $ RLP.encode
         $ RLP.RlpList [addrRlp, nonceRlp]
  in HexEthAddr $ toHex $ BS.drop (32-20) $ keccak256 bs

getTxs = sortTxs . rebTransactions

traceLogFromJSON :: Value -> RpcTraceLog
traceLogFromJSON = (\(Success a) -> a) . fromJSON

traceTxTree :: [RpcTraceLog] -> [Tree RpcTraceLog]
traceTxTree = traceTxTree' [[]] 1
  where
    traceTxTree' [[]] _ [] = []
    traceTxTree' (forest:[]) _ [] = reverse forest
    traceTxTree' (forest:calls) depth (t@(RpcTraceLog d me _ _ _ op _ _ _):ts)
      | depth == d = traceTxTree' ((Node t []:forest):calls) d ts
      | depth < d = traceTxTree' ([Node t []]:forest:calls) d ts
      | depth > d = let (Node tp _:forest') = head calls
                        calls' = tail calls
                    in traceTxTree' ((Node t []:Node tp (reverse forest):forest'):calls') d ts

-- TODO
updateDb doTest myUrl myPort ethUrl doPar mIniBlk numBlks iniDb = do
  --createDb
  (greet,myCon) <- connectDetail (defConInfo myUrl myPort)
  printErr greet
  when iniDb $ initDb ethUrl myCon
  dbInsertBlocks doTest mIniBlk numBlks ethUrl myCon doPar
  close myCon

initDb :: String -> MySQLConn -> IO ()
initDb ethUrl myCon = do
  dbCreateTables myCon
  dbInsertBlock0 ethUrl myCon

fromRight _ (Right r) = r
fromRight t (Left e) = error $ show t ++ ": " ++ show e

addrN :: (Integral n) => n -> HexEthAddr
addrN = HexEthAddr . toHex . bytesN
      . (fromUIntN :: Uint160 -> Bytes20)
      . (fromInteger :: Integer -> Uint160)
      . fromIntegral

addr0 = addrN 0
addr1 = addrN 1
addr2 = addrN 2
addr3 = addrN 3
addr4 = addrN 4

data EthProto = Frontier
              | Homestead
              | DaoFork
              | TangerineWhistle
              | SpuriousDragon
              | Byzantium
              deriving (Enum, Eq, Show)

data EthProtoCfg = EthProtoCfg
  { protoHomestead :: BlockNum
  , protoDAO :: BlockNum
  , protoTangerine :: BlockNum
  , protoSpurious :: BlockNum
  , protoByzantium :: BlockNum
  } deriving (Show)

publicEthProtoCfg = EthProtoCfg 1150000 1920000 2463000 2675000 4370000

ethProto :: EthProtoCfg -> BlockNum -> EthProto
ethProto (EthProtoCfg homestead daofork tangerine spurious byzantium) blkNum =
  let protos = [Byzantium,SpuriousDragon,TangerineWhistle,DaoFork,Homestead,Frontier]
      blkNums = [byzantium, spurious, tangerine, daofork, homestead, 0]
  in fst $ head $ filter ((blkNum>=) . snd) $ zip protos blkNums

dbInsertBlock0 :: String -> MySQLConn -> IO ()
dbInsertBlock0 ethUrl myCon = do
  accs <- stateAccounts . fromRight "debug_dumpBlock"
      <$> runWeb3 False ethUrl (debug_dumpBlock 0)
  mapM_ (\acc -> insertGenesis myCon (accAddr acc) (accBalance acc)) accs

-- TODO
dbInsertBlocks doTest mIniBlk numBlks ethUrl myCon doPar = do
  iniBlk <- maybe ((+1) <$> selectLatestBlock myCon) return mIniBlk
  let blks = map (iniBlk+) [0 .. numBlks-1]
  mapM_ (dbInsertBlock doTest ethUrl myCon doPar) blks

ignoreCtrlC :: IO a -> IO a
ignoreCtrlC f = do
  oldH <- installHandler keyboardSignal Ignore Nothing
  printErr "Ignorar Ctrl+C"
  r <- f
  printErr "Restaurar Ctrl+C"
  installHandler keyboardSignal oldH Nothing
  return r

dbInsertBlock doTest ethUrl myCon doPar blkNum = do
  blk <- fromJust . fromRight "eth_getBlockByNumber"
     <$> runWeb3 False ethUrl (eth_getBlockByNumber (RPBNum blkNum) True)
  let mtx = MyBlock blkNum (fromJust $ rebHash blk)
                    (fromJust $ rebMiner blk) (rebDifficulty blk)
                    (rebGasLimit blk)
  myDbTxs1 <- mapM (dbInsertTx ethUrl . (\(POObject tx) -> tx)) (getTxs blk)
  let myDbTxs2 = parItxs doPar myDbTxs1
  let (!rTx,!rNew,!rCall,!rItx,!rDacc) = force (spanDbTxs myDbTxs2)
  if doTest
    then do
      print mtx
      mapM_ (\(POObject tx) -> do
        txTrace <- getTraceTx ethUrl (btxHash tx)
        let tls = traceValueTxLogs txTrace
        let rtls = snd $ reduceTraceLogs tls
        --print tls
        print rtls
        print $ traceTxTree rtls
        ) (getTxs blk)
    else
      ignoreCtrlC $ withTransaction myCon $ do
        dbInsertMyTx myCon mtx
        insertTxs myCon rTx
        insertContractCreations myCon rNew
        insertMsgCalls myCon rCall
        insertInternalTxs myCon rItx
        mapM_ (dbInsertMyTouchedAccount ethUrl myCon) (nub rDacc)

spanDbTxs = reverseDbTxs . foldl spanMyTxs ([],[],[],[],[])
reverseDbTxs (rTx,rNew,rCall,rItx,rDacc) =
  ( reverse rTx
  , reverse rNew
  , reverse rCall
  , reverse rItx
  , rDacc
  )
spanMyTxs r@(rTx,rNew,rCall,rItx,rDacc) (mtxs,mdas) =
  let (rTx',rNew',rCall',rItx',_) = foldl spanMyTx r mtxs
  in (rTx',rNew',rCall',rItx',rDacc++mdas)
spanMyTx (rTx,rNew,rCall,rItx,rDacc) mtx = case mtx of
  MyTx {} -> (mtx:rTx,rNew,rCall,rItx,rDacc)
  MyContractCreation {} -> (rTx,mtx:rNew,rCall,rItx,rDacc)
  MyMsgCall {} -> (rTx,rNew,mtx:rCall,rItx,rDacc)
  MyInternalTx {} -> (rTx,rNew,rCall,mtx:rItx,rDacc)
  _ -> error $ "spanMyTx: " ++ show mtx

dbInsertMyTxs ethUrl myCon (mtxs,mdas) = do
  mapM_ (dbInsertMyTx myCon) mtxs
  mapM_ (dbInsertMyTouchedAccount ethUrl myCon) mdas

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

nullAddr = (==addr0)

isReservedAddr addr = addr `elem` [addr0,addr1,addr2,addr3,addr4]

parItxs doPar myDbTxs =
  let mDbItxs1 = map (\(_,_,_,(mtxs,_)) -> mtxs) myDbTxs
      mDbItxs4 = if doPar
                    then
                      let mDbItxs2 = filter (not . null) mDbItxs1
                          forceList = withStrategy (parList rdeepseq)
                          mDbItxs3 = map forceList mDbItxs2
                      in unFilterNull [] mDbItxs1 mDbItxs3
                    else mDbItxs1
  in joinDbItxs [] myDbTxs mDbItxs4
  where
    unFilterNull r [] _ = reverse r
    unFilterNull r ([]:mtxss1) mtxs = unFilterNull ([]:r) mtxss1 mtxs
    unFilterNull r (_:mtxss1) (mtxs2:mtxss2) = unFilterNull (mtxs2:r) mtxss1 mtxss2
    joinDbItxs r [] [] = reverse r
    joinDbItxs r ((mtx1,mtx2,mdas2,(_,mdas3)):dbTxs) (mtxs3:dbItxs) =
      let mdas = if null mdas2 then mdas3 else head mdas2:mdas3
      in joinDbItxs ((mtx1:mtx2:mtxs3,mdas):r) dbTxs dbItxs

dbInsertTx ethUrl (RpcEthBlkTx txHash _ _ (Just blkNum) (Just txIdx) from mto txValue _ txGas _ _ _ _) = do
  txTrace <- getTraceTx ethUrl txHash
  let failed1 = traceValueTxFailed txTrace
  let (mop,tls) = if failed1
                    then (0,[])
                    else reduceTraceLogs $ traceValueTxLogs txTrace
  let failed = failed1 || hasOpInvalid mop
  let mtx1 = MyTx blkNum txIdx txHash txValue txGas failed mop
  (mtx2,mdas2,cAddr) <- case mto of
    Nothing -> do
      cAddr <- fromJust . txrContractAddress . fromJust
             . fromRight "eth_getTransactionReceipt"
           <$> runWeb3 False ethUrl (eth_getTransactionReceipt txHash)
      let mtx = MyContractCreation blkNum txIdx from cAddr
      return (mtx,[],cAddr)
    Just to -> do
      let mtx = MyMsgCall blkNum txIdx from to
      let mda = MyTouchedAccount blkNum txIdx to
      return (mtx,[mda],to)
  let (mtxs3,mdas3) = if not failed
                        then dbInsertInternalTxs
                                blkNum txIdx cAddr $ traceTxTree tls
                        else ([],[])
  --let mdas = if null mdas2 then mdas3 else head mdas2:mdas3
  --return (mtx1:mtx2:mtxs3,mdas)
  return (mtx1,mtx2,mdas2,(mtxs3,mdas3))

reduceTraceLogs :: [Value] -> (Word64,[RpcTraceLog])
reduceTraceLogs values =
  let (ops,rtls) = reduceTraceLogs' ([],[]) values
  in (traceMaskOps ops,rtls)
  where
    reduceTraceLogs' (ops,rtls) [] = (reverse ops, reverse rtls)
    reduceTraceLogs' (ops,rtls) (val:vals) =
      let tl = traceLogFromJSON val
          op = fromText $ traceLogOp tl
          isRtl = op `elem` reducedOps
      in reduceTraceLogs' (op:ops,if isRtl then tl:rtls else rtls) vals
    reducedOps =
      [ OpSTOP
      , OpCALL
      , OpCALLCODE
      , OpRETURN
      , OpDELEGATECALL
      , OpSTATICCALL
      , OpREVERT
      , OpINVALID
      , OpCREATE
      , OpSELFDESTRUCT
      , OpBALANCE
      , OpEXTCODESIZE
      , OpEXTCODECOPY
      , OpSLOAD
      , OpSSTORE
      ]

dbInsertInternalTxs :: BlockNum -> Int
                    -> HexEthAddr -> [Tree RpcTraceLog]
                    -> ([MysqlTx],[MysqlTx])
dbInsertInternalTxs blkNum txIdx cAddr treeTraceLogs =
  let (rs,tchs,_,_,_) = getItxs 0 cAddr treeTraceLogs
      itxs = reverse $ concatMaybes rs
      tchs' = nub $ concatMaybes tchs
      mtxs = map dbInsertInternalTx itxs
      mdas = map (MyTouchedAccount blkNum txIdx) tchs'
  in (mtxs,mdas)
  where
    dbInsertInternalTx (idx,from,to,opcode) =
      MyInternalTx blkNum txIdx idx from to opcode
    getItxs idx addr forest = foldl getItx ([],[],idx,addr,forest) forest
    getItx (r,tchs,idx,addr,_:ts) t =
      let (RpcTraceLog _ _ _ _ _ opStr _ mstack _) = rootLabel t
          op = fromText opStr
          (addr',itx,tch) = case op of
            OpCALL ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (to',Just (idx,addr,to',opCall),tch)
            OpCALLCODE ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',opCallcode),tch)
            OpDELEGATECALL ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',opDelegatecall),tch)
            OpSTATICCALL ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (to',Just (idx,addr,to',opStaticcall),tch)
            {- OpREVERT -> -}
            OpCREATE ->
              let t' = rootLabel $ head ts
                  (to:_) = getStack (traceLogStack t')
                  (to',_) = stackAddr' to
              in (to',Just (idx,addr,to',opCreate),Nothing)
            OpSELFDESTRUCT ->
              let (to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',opSelfdestruct),tch)
            OpBALANCE ->
              let (acc:_) = getStack mstack
                  (_,tch) = stackAddr' acc
              in (addr,Nothing,tch)
            OpEXTCODESIZE ->
              let (acc:_) = getStack mstack
                  (_,tch) = stackAddr' acc
              in (addr,Nothing,tch)
            OpEXTCODECOPY ->
              let (acc:_) = getStack mstack
                  (_,tch) = stackAddr' acc
              in (addr,Nothing,tch)
            OpSLOAD -> (addr,Nothing,Just addr)
            OpSSTORE -> (addr,Nothing,Just addr)
            _ -> (addr,Nothing,Nothing)
          (fItxs,fTchs,nIdx,_,_) = getItxs (idx+1) addr' (subForest t)
          r' = concatRs fItxs (itx:r)
          tchs' = concatRs fTchs (tch:tchs)
      in (r',tchs',nIdx+1,addr,ts)
    concatRs rs1 rs2 = if null rs1 then rs2 else rs1++rs2
    concatMaybes = map fromJust . filter isJust
    getStack = reverse . fromJust
    stackAddr' hexD =
      let addr = stackAddr hexD
          tch = if nullAddr addr then Nothing else Just addr
      in (addr,tch)
    stackAddr = HexEthAddr . joinHex . T.drop (64-40) . stripHex

opCreate = toOpcode OpCREATE
opCall = toOpcode OpCALL
opCallcode = toOpcode OpCALLCODE
opDelegatecall = toOpcode OpDELEGATECALL
opStaticcall = toOpcode OpSTATICCALL
opRevert = toOpcode OpREVERT
opInvalid = toOpcode OpINVALID
opSelfdestruct = toOpcode OpSELFDESTRUCT

hasOpInvalid = (/=0) . (traceMaskOps [OpINVALID] .&.)

traceLogsMaskOp :: [RpcTraceLog] -> Word64
traceLogsMaskOp = traceMaskOps . map (fromText . traceLogOp)

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

dbInsertMyTouchedAccount ethUrl myCon mtx = case mtx of
  (MyTouchedAccount blkNum txIdx addr) -> do
    let proto = ethProto publicEthProtoCfg blkNum
    when (proto `elem` enumFrom SpuriousDragon) $ do
      yetIs <- isJust <$> selectDeadAccountAddr myCon addr
      unless yetIs $ do
        isDead <- isDeadAccount ethUrl myCon blkNum txIdx addr
        when isDead $ insertDeadAccount myCon blkNum txIdx addr
  _ -> error $ "dbInsertMyTouchedAccount: " ++ show mtx

isDeadAccount ethUrl myCon blkNum txIdx addr =
  if isReservedAddr addr
    then return False
    else isEmptyAccount ethUrl myCon blkNum txIdx addr

isEmptyAccount ethUrl myCon blkNum txIdx addr = do
  balance <- maybe 0 id . fromRight "eth_getBalance'"
         <$> runWeb3 False ethUrl (eth_getBalance' addr $ RPBNum blkNum)
  if balance /= 0
    then return False
    else do
      hasCode <- (/="0x") . fromRight "eth_getCode"
             <$> runWeb3 False ethUrl (eth_getCode addr $ RPBNum blkNum)
      if hasCode
        then return False
        else not <$> accountHasNonce myCon blkNum txIdx addr

accountHasNonce myCon blkNum txIdx addr = do
  hasNonce <- selectMsgCallHasFrom myCon blkNum txIdx addr
  if hasNonce
    then return True
    else selectContractCreationHasFrom myCon blkNum txIdx addr

accountNonce myCon addr = (+)
                      <$> selectMsgCallCountFrom myCon addr
                      <*> selectContractCreationCountFrom myCon addr

getTraceTx ethUrl txHash = fromRight "debug_traceTransactionValue"
                       <$> runWeb3 False ethUrl
                            (debug_traceTransactionValue txHash
                                  (defaultTraceOptions
                                      { traceOpDisableStorage = True
                                      , traceOpDisableMemory = True
                                      }))

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
  execute_ myCon createLastBlkQ >>= printErr

createDb :: IO ()
createDb = do
  (greet,myCon) <- connectDetail rootConInfo
  print greet
  --command myCon (COM_INIT_DB "ethdb") >>= putStrLn . show
  execute_ myCon "create database `ethdb`;" >>= print
  execute_ myCon "create user 'kitten' identified by 'kitten';" >>= print
  execute_ myCon "grant usage on *.* to 'kitten'@'%' identified by 'kitten';" >>= print
  execute_ myCon "grant all privileges on `ethdb`.* to 'kitten'@'%';" >>= print
  execute_ myCon "flush privileges;" >>= print
  close myCon

