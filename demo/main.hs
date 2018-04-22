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
import Data.Ethereum.EthProto
import Data.Ethereum.EvmDisasm
import Data.Ethereum.EvmOp
import qualified Data.Ethereum.RLP as RLP
import Data.Maybe (fromJust, isJust)
import Data.List (groupBy,nub,nubBy,sort,sortBy)
import Data.Monoid ((<>))
import Data.Ord (comparing)
import qualified Data.Text as T
import Data.Tree
import Data.Word (Word8,Word32,Word64)
import Database.MySQL.Base
import Database.MySQL.Ethereum.DB
import Network.JsonRpcConn (LogLevel(..), filterLoggerLogLevel)
import Network.Web3
import Network.Web3.Dapp.Bytes
import Network.Web3.Dapp.EthABI (keccak256)
import Network.Web3.Dapp.EthABI.Bzz (getBinBzzr0)
import Network.Web3.Dapp.Int
import System.Environment (getArgs,getProgName)
import System.IO
  ( BufferMode(..)
  , hGetContents
  , hPrint
  , hPutStrLn
  , hSetBuffering
  , stderr
  , stdin
  , stdout
  )
import qualified System.IO.Streams as IOS
import System.Posix.Signals

getOps :: IO (String,Integer,String,Maybe String,Maybe BlockNum,BlockNum,Bool,Bool,Bool,Bool)
getOps = do
  prog <- getProgName
  go prog ("localhost",3306,"http://localhost:8545",Nothing,Nothing,100,False,False,False,False) <$> getArgs
  where
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--myHttp":a:as) = go p (a,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--myPort":a:as) = go p (myUrl,read a,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--ethHttp":a:as) = go p (myUrl,myPort,a,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--cmd":a:as) = go p (myUrl,myPort,ethUrl,Just a,mIniBlk,numBlks, iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--iniBlk":a:as) = go p (myUrl,myPort,ethUrl,mCmd,Just $ read a, numBlks, iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--numBlks":a:as) = go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,read a,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--initDb":as) = go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks, True,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--par":as) = go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks, iniDb,True,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--log":as) = go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks, iniDb,doPar,True,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--test":as) = go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks, iniDb,doPar,doLog,True) as
    go p _ ("-h":as) = msgUso p
    go p _ ("--help":as) = msgUso p
    go _ r [] = r
    msgUso p = error $ p ++ " [-h|--help] [--myHttp myUrl] [--myPort <port>] [--ethHttp ethUrl] [--cmd <cmd>] [--iniBlk <num>] [--numBlks <num>] [--initDb] [--par] [--log] [--test]"

putStrLnErr = hPutStrLn stderr

printErr :: (Show a) => a -> IO ()
printErr = hPrint stderr

main :: IO ()
main = do
  (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) <- getOps
  case mCmd of
    Nothing -> updateDb doTest myUrl myPort ethUrl doPar mIniBlk numBlks iniDb
    Just cmd -> case cmd of
      "disasm" -> disasmStdin
      "updateCode" -> updateCodeDb doTest myUrl myPort ethUrl mIniBlk numBlks iniDb
      _ -> error $ "Comando no reconocido: " ++ cmd
  {-if doTest
    then tests myUrl myPort ethUrl iniBlk numBlks doLog
    else updateDb doTest myUrl myPort ethUrl doPar iniBlk numBlks iniDb-}

disasmStdin = hGetContents stdin
           >>= mapM_ print . fromRight "parseEvmCode" . parseEvmCode . read

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

getTxs = sortTxs . rebTransactions

updateDb doTest myUrl myPort ethUrl doPar mIniBlk numBlks iniDb = do
  (greet,myCon) <- connectDetail (defConInfo myUrl myPort)
  printErr greet
  when iniDb $ initDb ethUrl myCon
  insertBlocksDb doTest mIniBlk numBlks ethUrl myCon doPar
  close myCon

initDb :: String -> MySQLConn -> IO ()
initDb ethUrl myCon = do
  dbCreateTables myCon
  dbCreateTableContractCode myCon
  insertBlockGenesis ethUrl myCon

insertBlockGenesis :: String -> MySQLConn -> IO ()
insertBlockGenesis ethUrl myCon = do
  accs <- stateAccounts <$> dumpBlock ethUrl 0
  mapM_ (\acc -> dbInsertGenesis myCon (accAddr acc) (accBalance acc)) accs

insertBlocksDb doTest mIniBlk numBlks ethUrl myCon doPar = do
  iniBlk <- maybe (maybe 1 (+1) <$> dbSelectLatestBlockNum myCon) return mIniBlk
  let blks = map (iniBlk+) [0 .. numBlks-1]
  mapM_ (insertBlockDb doTest ethUrl myCon doPar) blks

ignoreCtrlC :: IO a -> IO a
ignoreCtrlC f = do
  oldH <- installHandler keyboardSignal Ignore Nothing
  printErr "Ignorar Ctrl+C"
  r <- f
  printErr "Restaurar Ctrl+C"
  installHandler keyboardSignal oldH Nothing
  return r

insertBlockDb doTest ethUrl myCon doPar blkNum = do
  blk <- getBlockByNumber ethUrl blkNum
  let mtx = MyBlock blkNum (fromJust $ rebHash blk)
                    (fromJust $ rebMiner blk) (rebDifficulty blk)
                    (rebGasLimit blk)
  myDbTxs1 <- mapM (getDbTx ethUrl . (\(POObject tx) -> tx)) (getTxs blk)
  let myDbTxs2 = parallelizeItxs doPar myDbTxs1
  let (!rTx,!rNew,!rCall,!rItx,!rDacc) = force (spanDbTxs myDbTxs2)
  if doTest
    then do
      print mtx
      mapM_ (\(POObject tx) -> do
        txTrace <- getTraceTx ethUrl (btxHash tx)
        let tls = traceValueTxLogs txTrace
        let rtls = snd $ reduceTraceLogs tls
        print "1 --------------------------------------------------------"
        mapM_ print tls
        print "2 --------------------------------------------------------"
        mapM_ (print . tl2tup) rtls
        print "3 --------------------------------------------------------"
        putStrLn $ drawForest $ map (fmap (show . tl2tup)) $ traceTxTree rtls
        ) (getTxs blk)
      let txs = sort $ nub $ map (\(MyTouchedAccount _ txIdx _) -> txIdx) rDacc
      let txAddrs = map (\txIdx -> (txIdx,map (\(MyTouchedAccount _ _ to) -> to) $ filter (\(MyTouchedAccount _ txIdx' _) -> txIdx == txIdx') rDacc)) txs
      rDacc' <- mapM (\(txIdx,addrs) -> map (MyTouchedAccount blkNum txIdx)
                                     <$> deadAccounts ethUrl myCon blkNum txIdx addrs
                  ) txAddrs
      mapM_ print rDacc'
    else
      ignoreCtrlC $ do
        withTransaction myCon $ do
          dbInsertMyTx myCon mtx
          dbInsertTxs myCon rTx
          dbInsertContractCreations myCon rNew
          dbInsertMsgCalls myCon rCall
          dbInsertInternalTxs myCon rItx
          insertMyTouchedAccountsDb ethUrl myCon blkNum rDacc
        withTransaction myCon $ do
          let mtxsNew = joinNews (rNew ++ filter (\(MyInternalTx _ _ _ _ _ opcode) -> opcode == opCreate) rItx)
          mapM_ (mapM_ (insertBlockContractCodeDb myCon ethUrl)) mtxsNew
  where
    tl2tup tl = (traceLogDepth tl, traceLogOp tl)

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

nullAddr = (==addr0)

parallelizeItxs doPar myDbTxs =
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

getDbTx ethUrl (RpcEthBlkTx txHash _ _ (Just blkNum) (Just txIdx) from mto txValue _ txGas _ _ _ _) = do
  txTrace <- getTraceTx ethUrl txHash
  let failed1 = traceValueTxFailed txTrace
  let (mop,tls) = if failed1
                    then (0,[])
                    else reduceTraceLogs $ traceValueTxLogs txTrace
  let failed = failed1 || mop `hasOp` OpINVALID
  let mtx1 = MyTx blkNum txIdx txHash txValue txGas failed mop
  (mtx2,mdas2,cAddr) <- case mto of
    Nothing -> do
      cAddr <- fromJust . txrContractAddress
           <$> getTransactionReceipt ethUrl txHash
      let mtx = MyContractCreation blkNum txIdx from cAddr
      return (mtx,[],cAddr)
    Just to -> do
      let mtx = MyMsgCall blkNum txIdx from to
      let mda = MyTouchedAccount blkNum txIdx to
      return (mtx,[mda],to)
  let (mtxs3,mdas3) = if not failed && not (mop `hasOp` OpREVERT)
                        then getDbInternalTxs
                                blkNum txIdx cAddr $ traceTxTree tls
                        else ([],[])
  return (mtx1,mtx2,mdas2,(mtxs3,mdas3))

valuesMaskOps :: [Value] -> Word64
valuesMaskOps = traceMaskOps . map valueOp
  where
    valueOp = fromText . traceLogOp . traceLogFromJSON

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
                        ts' = if depth - 1 == d
                                then ts
                                else t:ts
                    in traceTxTree' ((Node t []:Node tp (reverse forest):forest'):calls') (depth - 1) ts'

reduceTraceLogs :: [Value] -> (Word64,[RpcTraceLog])
reduceTraceLogs values =
  let tls = map traceLogFromJSON values
  in (traceMaskOps $ map (fromText . traceLogOp) tls,tls)
{-reduceTraceLogs = reduceTraceLogs' (True,[])
  where
    reduceTraceLogs' (es1,rtls) [] = reverse rtls
    reduceTraceLogs' (es1,rtls) (val:vals) =
      let isRtl = if es1
                    then True
                    else let tl = traceLogFromJSON val
                             op = fromText $ traceLogOp tl
                         in op `elem` reducedOps
      in reduceTraceLogs' (if isRtl then tl:rtls else rtls) vals
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
-}

getDbInternalTxs :: BlockNum -> Int -> HexEthAddr -> [Tree RpcTraceLog]
                 -> ([MysqlTx],[MysqlTx])
getDbInternalTxs blkNum txIdx cAddr treeTraceLogs =
  let (rs,tchs,_,_,_) = getItxs 0 cAddr treeTraceLogs
      itxs = reverse $ concatMaybes rs
      tchs' = nub $ concatMaybes tchs
      mtxs = map getDbInternalTx itxs
      mdas = map (MyTouchedAccount blkNum txIdx) tchs'
  in (mtxs,mdas)
  where
    getDbInternalTx (idx,from,to,opcode) =
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

insertMyTouchedAccountsDb ethUrl myCon blkNum rDacc = do
  let proto = ethProto publicEthProtoCfg blkNum
  when (proto `elem` enumFrom SpuriousDragon) $ do
    let rDacc1 = nubBy (\(MyTouchedAccount _ _ to1) (MyTouchedAccount _ _ to2) -> to1 == to2) rDacc
    let txIdxs = sort $ nub $ map (\(MyTouchedAccount _ txIdx _) -> txIdx) rDacc1
    let txIdxAddrs = map (\txIdx -> (txIdx,nub $ map (\(MyTouchedAccount _ _ to) -> to) $ filter (\(MyTouchedAccount _ txIdx' _) -> txIdx == txIdx') rDacc1)) txIdxs
    rDacc2 <- concat <$> mapM (\(txIdx,addrs) ->
                if null addrs
                  then return []
                  else do
                    addrs1 <- deadAccounts ethUrl myCon blkNum txIdx addrs
                    if null addrs1
                      then return []
                      else do
                        addrs2 <- dbSelectDeadAccountAddrs myCon addrs1
                        let addrs3 = filter (\addr -> not $ addr `elem` addrs2) addrs1
                        return $ map (MyTouchedAccount blkNum txIdx) addrs3
                ) txIdxAddrs
    unless (null rDacc2) $ dbInsertDeadAccounts myCon rDacc2

insertMyTouchedAccountDb ethUrl myCon mtx = case mtx of
  (MyTouchedAccount blkNum txIdx addr) -> do
    let proto = ethProto publicEthProtoCfg blkNum
    when (proto `elem` enumFrom SpuriousDragon) $ do
      yetIs <- isJust <$> dbSelectDeadAccountAddr myCon addr
      unless yetIs $ do
        isDead <- isDeadAccount ethUrl myCon blkNum txIdx addr
        when isDead $ dbInsertDeadAccount myCon blkNum txIdx addr
  _ -> error $ "dbInsertMyTouchedAccount: " ++ show mtx

isReservedAddr addr = addr `elem` [addr0,addr1,addr2,addr3,addr4]

deadAccounts :: String -> MySQLConn -> BlockNum -> Int
             -> [HexEthAddr] -> IO [HexEthAddr]
deadAccounts ethUrl myCon blkNum txIdx addrs = do
  let addrs1 = filter (not . isReservedAddr) addrs
  addrs2 <- map fst . filter ((==0) . snd)
        <$> mapM (\addr -> getBalance ethUrl blkNum addr
                            >>= \bal -> return (addr,bal)) addrs1
  addrs3 <- map fst . filter ((=="0x") . snd)
        <$> mapM (\addr -> getCode ethUrl blkNum addr
                            >>= \c -> return (addr,c)) addrs2
  addrs4 <- if null addrs3
              then return []
              else do
                addrs4a <- dbSelectMsgCallsFroms myCon blkNum txIdx addrs3
                return $ filter (\addr -> not $ addr `elem` addrs4a) addrs3
  if null addrs4
    then return []
    else do
      addrs5a <- dbSelectContractCreationFroms myCon blkNum txIdx addrs4
      return $ filter (\addr -> not $ addr `elem` addrs5a) addrs4

isDeadAccount ethUrl myCon blkNum txIdx addr =
  if isReservedAddr addr
    then return False
    else isEmptyAccount ethUrl myCon blkNum txIdx addr

isEmptyAccount ethUrl myCon blkNum txIdx addr = do
  balance <- getBalance ethUrl blkNum addr
  if balance /= 0
    then return False
    else do
      hasCode <- (/="0x") <$> getCode ethUrl blkNum addr
      if hasCode
        then return False
        else not <$> accountHasNonce myCon blkNum txIdx addr

accountHasNonce myCon blkNum txIdx addr = do
  hasNonce <- dbSelectMsgCallHasFrom myCon blkNum txIdx addr
  if hasNonce
    then return True
    else dbSelectContractCreationHasFrom myCon blkNum txIdx addr

accountNonce myCon addr = (+)
                      <$> dbSelectMsgCallCountFrom myCon addr
                      <*> dbSelectContractCreationCountFrom myCon addr

updateCodeDb doTest myUrl myPort ethUrl mIniBlk numBlks iniDb = do
  (greet,myCon) <- connectDetail (defConInfo myUrl myPort)
  printErr greet
  when iniDb $ dbCreateTableContractCode myCon
  iniBlk <- maybe (maybe 1 (+1) <$> dbSelectContractCodeLastBlkNum myCon) return mIniBlk
  txsNew <- dbSelectContractCreationFromBlkNum myCon (iniBlk+1) (iniBlk+numBlks)
  itxsNew <- dbSelectInternalTxFromBlkNum myCon (iniBlk+1) (iniBlk+numBlks) opCreate
  let blksMtxsNew = joinNews (txsNew ++ itxsNew)
  mapM_ (\blkMtxs -> ignoreCtrlC
                   $ withTransaction myCon
                   $ mapM_ (insertBlockContractCodeDb myCon ethUrl) blkMtxs
    ) blksMtxsNew
  close myCon

joinNews = map (nubBy eqMtxBlkNumAddr)
         . groupBy eqMtxBlkNum
         . sortBy (comparing mtxIdx)
  where
    eqMtxBlkNumAddr mtx1 mtx2 = (mtxBlkNumAddr mtx1) == (mtxBlkNumAddr mtx2)
    mtxBlkNumAddr mtx = case mtx of
      MyContractCreation blkNum _ _ addr -> (blkNum,addr)
      MyInternalTx blkNum _ _ _ addr _ -> (blkNum,addr)
      _ -> error $ "mtxBlkNumAddr: " ++ show mtx
    eqMtxBlkNum mtx1 mtx2 = (mtxBlkNum mtx1) == (mtxBlkNum mtx2)
    mtxBlkNum mtx = case mtx of
      MyContractCreation blkNum _ _ _ -> blkNum
      MyInternalTx blkNum _ _ _ _ _ -> blkNum
      _ -> error $ "mtxBlkNum: " ++ show mtx
    mtxIdx mtx = case mtx of
      MyContractCreation blkNum txIdx _ _ -> (blkNum,txIdx,0)
      MyInternalTx blkNum txIdx idx _ _ _ -> (blkNum,txIdx,idx+1)
      _ -> error $ "mtxIdx: " ++ show mtx

insertBlockContractCodeDb myCon ethUrl mtx = do
  let (blkNum,txIdx,addr) = mtxGetCodeInfo mtx
  failedTx <- dbSelectTxFailed myCon blkNum txIdx
  unless failedTx $ do
    code <- getCode ethUrl blkNum addr
    let mBzzr0 = getBinBzzr0 code
    dbInsertContractCode myCon blkNum addr mBzzr0 code

mtxGetCodeInfo mtx = case mtx of
  MyContractCreation blkNum txIdx _ addr -> (blkNum,txIdx,addr)
  MyInternalTx blkNum txIdx _ _ addr _ -> (blkNum,txIdx,addr)

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

contractAddress :: HexEthAddr -> Integer -> HexEthAddr
contractAddress addr nonce =
  let addrRlp = RLP.rlpEncode
              $ (fromHex :: T.Text -> BS.ByteString)
              $ getHexAddr addr
      nonceRlp = RLP.rlpEncode (nonce - 1)
      bs = RLP.rlpBs $ RLP.encode
         $ RLP.RlpList [addrRlp, nonceRlp]
  in HexEthAddr $ toHex $ BS.drop (32-20) $ keccak256 bs

getBalance ethUrl blkNum addr =
  maybe 0 id . fromRight "eth_getBalance'"
    <$> runWeb3 False ethUrl (eth_getBalance' addr $ RPBNum blkNum)

getCode ethUrl blkNum addr =
  fromRight "eth_getCode"
    <$> runWeb3 False ethUrl (eth_getCode addr $ RPBNum blkNum)

getTransactionReceipt ethUrl txHash =
  fromJust . fromRight "eth_getTransactionReceipt"
    <$> runWeb3 False ethUrl (eth_getTransactionReceipt txHash)

getBlockByNumber ethUrl blkNum = fromJust . fromRight "eth_getBlockByNumber"
                             <$> runWeb3 False ethUrl
                                   (eth_getBlockByNumber (RPBNum blkNum) True)
dumpBlock ethUrl blkNum = fromRight "debug_dumpBlock"
                      <$> runWeb3 False ethUrl (debug_dumpBlock blkNum)

getTraceTx ethUrl txHash = fromRight "debug_traceTransactionValue"
                       <$> runWeb3 False ethUrl
                            (debug_traceTransactionValue txHash
                                  (defaultTraceOptions
                                      { traceOpDisableStorage = True
                                      , traceOpDisableMemory = True
                                      }))

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

