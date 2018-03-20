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

import Control.Monad (filterM,unless,when)
import Control.Monad.IO.Class
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
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
import System.IO (BufferMode(..),hPutStrLn,hSetBuffering,stderr,stdout)
import qualified System.IO.Streams as IOS
import System.Posix.Signals

getOps :: IO (String,BlockNum,BlockNum,Bool,Bool)
getOps = do
  prog <- getProgName
  go prog ("http://192.168.122.201:8543",0,100,False,False) <$> getArgs
  where
    go p (url,iniBlk,numBlks,iniDb,doLog) ("--http":a:as) = go p (a,iniBlk,numBlks,iniDb,doLog) as
    go p (url,iniBlk,numBlks,iniDb,doLog) ("--iniBlk":a:as) = go p (url, read a, numBlks, iniDb,doLog) as
    go p (url,iniBlk,numBlks,iniDb,doLog) ("--numBlks":a:as) = go p (url, iniBlk,read a,iniDb,doLog) as
    go p (url,iniBlk,numBlks,iniDb,doLog) ("--initDb":as) = go p (url, iniBlk,numBlks, True,doLog) as
    go p (url,iniBlk,numBlks,iniDb,doLog) ("--log":as) = go p (url, iniBlk,numBlks, iniDb,True) as
    go p _ ("-h":as) = msgUso p
    go p _ ("--help":as) = msgUso p
    go _ r [] = r
    msgUso p = error $ p ++ " [-h|--help] [--http url] [--iniBlk <num>] [--numBlks <num>] [--initDb] [--log]"

defConInfo = defaultConnectInfo
  { ciUser = "kitten"
  , ciPassword = "kitten"
  , ciDatabase = "ethdb"
  }

rootConInfo = defaultConnectInfo { ciUser = "root", ciPassword = "" }

putStrLnErr = hPutStrLn stderr

printErr :: (Show a) => a -> IO ()
printErr = hPutStrLn stderr . show

data MysqlTx =
    MyBlock BlockNum HexHash256 HexEthAddr Integer Gas
  | MyTx BlockNum Int HexHash256 Integer Gas Bool Word64
  | MyContractCreation BlockNum Int HexEthAddr HexEthAddr
  | MyMsgCall BlockNum Int HexEthAddr HexEthAddr
  | MyInternalTx BlockNum Int Word32 HexEthAddr HexEthAddr Word8
  | MyTouchedAccount BlockNum Int HexEthAddr
  deriving (Eq,Show)

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
selectMsgCallHasFromQ = "select * from msgCalls where fromA = ? limit 1;"
selectMsgCallHasFromP addr = [mySetAddr addr]
selectMsgCallHasFrom myCon addr = do
  (colDefs,isValues) <- query myCon selectMsgCallHasFromQ
                                      (selectMsgCallHasFromP addr)
  not . null . fromJust <$> myReadAndSkipToEof isValues
selectMsgCallCountFromQ = "select count(*) from msgCalls where fromA = ?;"
selectMsgCallCountFromP addr = [mySetAddr addr]
selectMsgCallCountFrom myCon addr = do
  (colDefs,isValues) <- query myCon selectMsgCallCountFromQ
                                      (selectMsgCallCountFromP addr)
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
selectContractCreationHasFromQ = "select * from contractCreations where fromA = ? limit 1;"
selectContractCreationHasFromP addr = [mySetAddr addr]
selectContractCreationHasFrom myCon addr = do
  (colDefs,isValues) <- query myCon selectContractCreationHasFromQ
                                (selectContractCreationHasFromP addr)
  not . null . fromJust <$> myReadAndSkipToEof isValues
selectContractCreationCountFromQ = "select count(*) from contractCreations where fromA = ?;"
selectContractCreationCountFromP addr = [mySetAddr addr]
selectContractCreationCountFrom myCon addr = do
  (colDefs,isValues) <- query myCon selectContractCreationCountFromQ
                                (selectContractCreationCountFromP addr)
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

createLastBlkQ = "create table lastBlk (blkNum integer unsigned not null, primary key (blkNum));"
insertLastBlkQ = "insert into lastBlk (blkNum) value (?);"
lastBlkP blkNum = [mySetBlkNum blkNum]
selectLastBlkQ = "select blkNum from lastBlk order by blkNum desc limit 1;"
readLastBlk isValues = do
  myGetNum . head . fromJust <$> myReadAndSkipToEof isValues

runWeb3 doLog url f = runStderrLoggingT
                    $ filterLoggerLogLevel
                        (if doLog then LevelDebug else LevelOther "NoLogs")
                    $ runWeb3HttpT 5 5 url f

main :: IO ()
main = do
  (url,iniBlk,numBlks,iniDb,doLog) <- getOps
  --let addr = HexEthAddr "0x6a0a0fc761c612c340a0e98d33b37a75e5268472"
  --let nonces = [1..9999]
  --let cAddrs = map (contractAddress addr) nonces
  --mapM_ print $ zip cAddrs nonces
  --mapM_ (putStrLn . T.unpack) $ fromRight $ parseEvmCode "0x6004600c60003960046000f3600035ff00000000000000000000000000000000"
  --code <- fromRight <$> runWeb3 False url (eth_getCode (HexEthAddr "0x6a0a0fc761c612c340a0e98d33b37a75e5268472") RPBLatest)
  --mapM_ (putStrLn . T.unpack) $ fromRight $ parseEvmCode code
  --tests doLog url iniBlk numBlks
  updateDb url iniBlk numBlks iniDb

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

tests :: Bool -> String -> BlockNum -> BlockNum -> IO ()
tests doLog url iniBlk numBlks = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering
  let blks = map (iniBlk+) [0 .. numBlks-1]
  mapM_ (\blkNum -> do
    blk <- fromJust . fromRight
       <$> runWeb3 doLog url (eth_getBlockByNumber (RPBNum blkNum) True)
    let txs = getTxs blk
    mapM_ (\(POObject tx) -> do
      let txH = btxHash tx
      let txIdx = fromJust $ btxTransactionIndex tx
      print (blkNum,txIdx)
      traceTx <- getTraceTx url txH
      unless (traceTxFailed traceTx) $ do
        let traceLogs = traceTxLogs traceTx
        mapM_ (\trL -> do
          let trOp = traceLogOp trL
          when (testOp trOp) $ putStrLn $ show (toHex blkNum) ++ " " ++ show txIdx ++ " " ++ (show $ traceLogDepth trL) ++ " " ++ (T.unpack trOp) ++ " " ++ (if traceTxFailed traceTx then ("(failed) ") else "") ++ show (map T.unpack $ fromJust $ traceLogStack trL) ++ " " ++ show (traceLogMemory trL)
          ) traceLogs
        when (any ((\trOp -> trOp=="CREATE") . traceLogOp) traceLogs) $ do
          let treeTraceLogs = traceTxTree traceLogs
          -- putStrLnErr $ drawForest $ map (fmap show) $ treeTraceLogs
          mapM_ (putStrLn . show . snd) $ filterCreateOp treeTraceLogs
      ) txs
    ) blks
  where
    testOp trOp = case trOp of
      "CREATE" -> True
      "CALL" -> False
      "CALLCODE" -> True
      "DELEGATECALL" -> True
      "STATICCALL" -> True
      "REVERT" -> True
      "INVALID" -> True
      "SELFDESTRUCT" -> True
      _ -> False

traceTxTree :: [RpcTraceLog] -> [Tree RpcTraceLog]
traceTxTree = traceTxTree' [[]] 1
  where
    traceTxTree' (forest:[]) _ [] = reverse forest
    traceTxTree' (forest:calls) depth (t@(RpcTraceLog d me _ _ _ op _ _ _):ts)
      | depth == d = traceTxTree' ((Node t []:forest):calls) d ts
      | depth+1 == d = traceTxTree' ([Node t []]:forest:calls) d ts
      | depth-1 == d = let (Node tp _:forest') = head calls
                           calls' = tail calls
                       in traceTxTree' ((Node t []:Node tp (reverse forest):forest'):calls') d ts

filterCreateOp :: [Tree RpcTraceLog] -> [(Tree RpcTraceLog,HexEthAddr)]
filterCreateOp = reverse . fst . foldl filterCreateOp' ([],False)
  where
    filterCreateOp' (r,popVal) n@(Node t@(RpcTraceLog _ me _ _ _ op _ (Just stack) _) forest)
      | popVal == False =
          let forestCreateOps = filterCreateOp forest
          in case op of
            "CREATE" -> (appendOps (Just (n,addr0)) forestCreateOps r,True)
            _ -> (appendOps Nothing forestCreateOps r,popVal)
      | popVal == True =
          let (n',_) = head r
              r' = tail r
              forestCreateOps = filterCreateOp forest
          in (appendOps (Just (n',popAddr stack)) forestCreateOps r',False)
    popAddr = HexEthAddr . toHex . BS.drop (32-20) . fromHex . last
    appendOps mop forestOps r =
      if null forestOps
        then maybe r (:r) mop
        else let forest = maybe forestOps (:forestOps) mop
                 forestLen = length forest
             in if forestLen == 1 then (head forest:r) else (forest++r)

-- TODO
updateDb :: String -> BlockNum -> BlockNum -> Bool -> IO ()
updateDb url blkIni numBlks iniDb = do
  --createDb
  (greet,myCon) <- connectDetail defConInfo
  printErr greet
  when iniDb $ initDb url myCon
  dbInsertBlocks blkIni numBlks url myCon
  close myCon

dbGetLastBlk :: MySQLConn -> IO BlockNum
dbGetLastBlk myCon = do
  (colDefs,isValues) <- query_ myCon selectLastBlkQ
  readLastBlk isValues

dbInsertLastBlk :: MySQLConn -> BlockNum -> IO ()
dbInsertLastBlk myCon blkNum = execute myCon insertLastBlkQ (lastBlkP blkNum) >>= printErr >> return ()

initDb :: String -> MySQLConn -> IO ()
initDb url myCon = do
  dbCreateTables myCon
  dbInsertBlock0 url myCon
  dbInsertLastBlk myCon 0

fromRight (Right r) = r

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
dbInsertBlock0 url myCon = do
  accs <- stateAccounts . fromRight
      <$> runWeb3 False url (debug_dumpBlock 0)
  mapM_ (\acc -> insertGenesis myCon (accAddr acc) (accBalance acc)) accs

-- TODO
dbInsertBlocks :: BlockNum -> BlockNum -> String -> MySQLConn -> IO ()
dbInsertBlocks blk numBlks url myCon = do
  --blk <- dbGetLastBlk myCon
  let blks = map (blk+) [0 .. numBlks-1]
  mapM_ (dbInsertBlock url myCon) blks
  --dbInsertLastBlk myCon (last blks)

ignoreCtrlC :: IO a -> IO a
ignoreCtrlC f = do
  oldH <- installHandler keyboardSignal Ignore Nothing
  printErr "Ignorar Ctrl+C"
  r <- f
  printErr "Restaurar Ctrl+C"
  installHandler keyboardSignal oldH Nothing
  return r

dbInsertBlock :: String -> MySQLConn -> BlockNum -> IO ()
dbInsertBlock url myCon blkNum = do
  blk <- fromJust . fromRight
      <$> runWeb3 False url (eth_getBlockByNumber (RPBNum blkNum) True)
  let mtx = MyBlock blkNum (fromJust $ rebHash blk)
                    (fromJust $ rebMiner blk) (rebDifficulty blk)
                    (rebGasLimit blk)
  myDbTxs <- mapM (dbInsertTx url . (\(POObject tx) -> tx)) (getTxs blk)
  ignoreCtrlC $ do
    dbInsertMyTx myCon mtx
    mapM_ (dbInsertMyTxs url myCon) myDbTxs

dbInsertMyTxs url myCon (mtxs,mdas) = do
  mapM_ (dbInsertMyTx myCon) mtxs
  mapM_ (dbInsertMyTouchedAccount url myCon) mdas

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

isReservedAddr addr = any (==addr) [addr0,addr1,addr2,addr3,addr4]

dbInsertTx :: String -> RpcEthBlkTx -> IO ([MysqlTx],[MysqlTx])
dbInsertTx url (RpcEthBlkTx txHash _ _ (Just blkNum) (Just txIdx) from mto txValue _ txGas _ _ _ _) = do
  txTrace <- getTraceTx url txHash
  let failed = traceTxFailed txTrace
  let (mop,tls) = if failed
                    then (0,[])
                    else let tls = traceTxLogs txTrace
                         in (traceLogsMaskOp tls, tls)
  let mtx1 = MyTx blkNum txIdx txHash txValue txGas failed mop
  (mtx2,mdas2,cAddr) <- case mto of
    Nothing -> do
      cAddr <- fromJust . txrContractAddress . fromJust . fromRight
           <$> runWeb3 False url (eth_getTransactionReceipt txHash)
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
  let mdas = nub $ if null mdas2 then mdas3 else head mdas2:mdas3
  return (mtx1:mtx2:mtxs3,mdas)

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
    getItx (r,tchs,idx,addr,(_:ts)) t =
      let (RpcTraceLog _ _ _ _ _ op _ mstack _) = rootLabel t
          (addr',itx,tch) = case op of
            "CALL" ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (to',Just (idx,addr,to',opCall),tch)
            "CALLCODE" ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',opCallcode),tch)
            "DELEGATECALL" ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',opDelegatecall),tch)
            "CREATE" ->
              let t' = rootLabel $ head ts
                  (to:_) = getStack (traceLogStack t')
                  (to',_) = stackAddr' to
              in (to',Just (idx,addr,to',opCreate),Nothing)
            "SELFDESTRUCT" ->
              let (to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',opSelfdestruct),tch)
            "BALANCE" ->
              let (acc:_) = getStack mstack
                  (_,tch) = stackAddr' acc
              in (addr,Nothing,tch)
            "EXTCODESIZE" ->
              let (acc:_) = getStack mstack
                  (_,tch) = stackAddr' acc
              in (addr,Nothing,tch)
            "EXTCODECOPY" ->
              let (acc:_) = getStack mstack
                  (_,tch) = stackAddr' acc
              in (addr,Nothing,tch)
            "SLOAD" -> (addr,Nothing,Just addr)
            "SSTORE" -> (addr,Nothing,Just addr)
            _ -> (addr,Nothing,Nothing)
          (fItxs,fTchs,nIdx,_,_) = getItxs (idx+1) addr' (subForest t)
          r' = concatRs fItxs (itx:r)
          tchs' = concatRs fTchs (tch:tchs)
      in (r',tchs',nIdx+1,addr,ts)
    concatRs rs1 rs2 = if null rs1 then rs2 else (rs1++rs2)
    concatMaybes = map fromJust . filter isJust
    getStack = reverse . fromJust
    stackAddr' hexD =
      let addr = stackAddr hexD
          tch = if nullAddr addr then Nothing else Just addr
      in (addr,tch)
    stackAddr = HexEthAddr . joinHex . T.drop (64-40) . stripHex

opCall = 0xf1
opCallcode = 0xf2
opDelegatecall = 0xf4
opCreate = 0xf0
opSelfdestruct = 0xff

traceLogsMaskOp :: [RpcTraceLog] -> Word64
traceLogsMaskOp = traceMaskOps . map traceLogOp

traceMaskOps :: [Text] -> Word64
traceMaskOps = fst . foldl traceMaskOp (0,opcodeMap)
  where
    traceMaskOp (r,opMap) op =
      let (opms,opMap') = partitionOpMap ([],[]) op opMap
          r' = if null opms then r else r .|. (snd $ head opms)
      in (r', opMap')
    partitionOpMap (opms1,opms2) _ [] = (reverse opms1, reverse opms2)
    partitionOpMap (opms1,opms2) op (opm:opms) =
      let (opms1',opms2') = if op `elem` (fst opm)
                              then (opm:opms1,opms2)
                              else (opms1,opm:opms2)
      in partitionOpMap (opms1',opms2') op opms

maskGetOps :: Word64 -> [[Text]]
maskGetOps mop = map fst $ filter (\(_,m) -> (m .&. mop) /= 0) opcodeMap

opcodeMap = zip opcodeNoms (map bit [0..63])
opcodeNoms =
  [ ["STOP"]
  , ["ADD", "MUL", "SUB", "DIV", "SDIV", "MOD", "SMOD", "ADDMOD", "MULMOD", "EXP", "SIGNEXTEND"]
  , ["LT", "GT", "SLT", "SGT", "EQ", "ISZERO", "AND", "OR", "XOR", "NOT", "BYTE"]
  , ["SHA3"]
  , ["ADDRESS"]
  , ["BALANCE"]
  , ["ORIGIN"]
  , ["CALLER"]
  , ["CALLVALUE"]
  , ["CALLDATALOAD","CALLDATASIZE","CALLDATACOPY"]
  , ["CODESIZE","CODECOPY"]
  , ["GASPRICE"]
  , ["EXTCODESIZE","EXTCODECOPY"]
  , ["BLOCKHASH","NUMBER"]
  , ["COINBASE"]
  , ["TIMESTAMP"]
  , ["DIFFICULTY"]
  , ["GASLIMIT"]
  , ["POP"]
  , ["MLOAD","MSTORE","MSTORE8"]
  , ["SLOAD"]
  , ["SSTORE"]
  , ["JUMP","JUMPI","JUMPDEST"]
  , ["PC","MSIZE","GAS"]
  , ["PUSH1","PUSH2","PUSH3","PUSH4","PUSH5","PUSH6","PUSH7","PUSH8","PUSH9","PUSH10","PUSH11","PUSH12","PUSH13","PUSH14","PUSH15","PUSH16","PUSH17","PUSH18","PUSH19","PUSH20","PUSH21","PUSH22","PUSH23","PUSH24","PUSH25","PUSH26","PUSH27","PUSH28","PUSH29","PUSH30","PUSH31","PUSH32"]
  , ["DUP1","DUP2","DUP3","DUP4","DUP5","DUP6","DUP7","DUP8","DUP9","DUP10","DUP11","DUP12","DUP13","DUP14","DUP15","DUP16"]
  , ["SWAP1","SWAP2","SWAP3","SWAP4","SWAP5","SWAP6","SWAP7","SWAP8","SWAP9","SWAP10","SWAP11","SWAP12","SWAP13","SWAP14","SWAP15","SWAP16"]
  , ["LOG0","LOG1","LOG2","LOG3","LOG4"]
  , ["CREATE"]
  , ["CALL"]
  , ["CALLCODE"]
  , ["RETURN"]
  , ["DELEGATECALL"]
  , ["INVALID"]
  , ["SELFDESTRUCT"]
  ]

dbInsertMyTouchedAccount url myCon mtx = case mtx of
  (MyTouchedAccount blkNum txIdx addr) -> do
    yetIs <- isJust <$> selectDeadAccountAddr myCon addr
    unless yetIs $ do
      isDead <- isDeadAccount url myCon blkNum txIdx addr
      when isDead $ insertDeadAccount myCon blkNum txIdx addr
  _ -> error $ "dbInsertMyTouchedAccount: " ++ show mtx

isDeadAccount :: String -> MySQLConn
              -> BlockNum -> Int -> HexEthAddr -> IO Bool
isDeadAccount url myCon blkNum txIdx addr = do
  if nullAddr addr
    then return False
    else do
      let proto = ethProto publicEthProtoCfg blkNum
      if proto `elem` enumFrom SpuriousDragon
        then if isReservedAddr addr
              then return False
              else isEmptyAccount url myCon blkNum addr
        else return False

isEmptyAccount :: String -> MySQLConn
               -> BlockNum -> HexEthAddr -> IO Bool
isEmptyAccount url myCon blkNum addr = do
  balance <- maybe 0 id . fromRight
         <$> runWeb3 False url (eth_getBalance' addr $ RPBNum blkNum)
  if balance /= 0
    then return False
    else do
      hasCode <- not . (==T.empty) . stripHex . fromRight
            <$> runWeb3 False url (eth_getCode addr $ RPBNum blkNum)
      if hasCode == True
        then return False
        else not <$> accountHasNonce myCon addr

accountHasNonce myCon addr = do
  hasNonce <- selectMsgCallHasFrom myCon addr
  if hasNonce
    then return True
    else selectContractCreationHasFrom myCon addr

accountNonce myCon addr = (+)
                      <$> selectMsgCallCountFrom myCon addr
                      <*> selectContractCreationCountFrom myCon addr

getTraceTx url txHash = fromRight <$> runWeb3 False url
                                        (debug_traceTransaction txHash
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
  putStrLn $ show greet
  --command myCon (COM_INIT_DB "ethdb") >>= putStrLn . show
  execute_ myCon "create database `ethdb`;" >>= putStrLn . show
  execute_ myCon "create user 'kitten' identified by 'kitten';" >>= putStrLn . show
  execute_ myCon "grant usage on *.* to 'kitten'@'%' identified by 'kitten';" >>= putStrLn . show
  execute_ myCon "grant all privileges on `ethdb`.* to 'kitten'@'%';" >>= putStrLn . show
  execute_ myCon "flush privileges;" >>= putStrLn . show
  close myCon

