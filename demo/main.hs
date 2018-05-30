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

import Control.Arrow ((&&&))
import Control.Concurrent (threadDelay)
import Control.DeepSeq (NFData(..),deepseq,force)
import Control.Monad (filterM,foldM,unless,when)
import Control.Monad.IO.Class
import Control.Parallel.Strategies (parList,rdeepseq,withStrategy)
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Either (isRight)
import Data.Ethereum.EthProto
import Data.Ethereum.EvmDisasm
import Data.Ethereum.EvmOp
import qualified Data.Ethereum.RLP as RLP
import qualified Data.HashMap.Lazy as HM
import Data.Maybe (fromJust, isJust)
import Data.List (groupBy,nub,nubBy,partition,sort,sortBy)
import Data.Monoid ((<>))
import Data.Ord (comparing)
import qualified Data.Text as T
import Data.Tree
import Data.Word (Word8,Word32,Word64)
import Database.MySQL.Base
import Database.MySQL.Ethereum.DB
import Network.JsonRpcCliHttp (Request)
import Network.JsonRpcConn (LogLevel(..), filterLoggerLogLevel)
import Network.Web3
import Network.Web3.Dapp.Bytes
import Network.Web3.Dapp.ERC20
import Network.Web3.Dapp.ERC20.Interface
import Network.Web3.Dapp.EthABI (keccak256,topicNull)
import Network.Web3.Dapp.EthABI.Bzz (getBinBzzr0)
import Network.Web3.Dapp.EthABI.Types
  ( Interface(IEvent)
  , ToCanonical(..)
  , isInterfaceEvent
  )
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

getOps :: IO (String,Integer,String,Maybe String,Maybe BlockNum,Maybe BlockNum,Bool,Bool,Bool,Bool)
getOps = do
  prog <- getProgName
  go prog ("localhost",3306,"http://localhost:8545",Nothing,Nothing,Nothing,False,False,False,False) <$> getArgs
  where
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--myHttp":a:as) = go p (a,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--myPort":a:as) = go p (myUrl,read a,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--ethHttp":a:as) = go p (myUrl,myPort,a,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--cmd":a:as) = go p (myUrl,myPort,ethUrl,Just a,mIniBlk,numBlks, iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--iniBlk":a:as) = go p (myUrl,myPort,ethUrl,mCmd,Just $ read a, numBlks, iniDb,doPar,doLog,doTest) as
    go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,numBlks,iniDb,doPar,doLog,doTest) ("--numBlks":a:as) = go p (myUrl,myPort,ethUrl,mCmd,mIniBlk,Just $ read a,iniDb,doPar,doLog,doTest) as
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
  (myUrl,myPort,ethUrl,mCmd,mIniBlk,mNumBlks,iniDb,doPar,doLog,doTest) <- getOps
  case mCmd of
    Nothing -> updateDb doTest myUrl myPort ethUrl doPar mIniBlk (maybe 100 id mNumBlks) iniDb
    Just cmd -> case cmd of
      "disasm" -> disasmStdin
      "updateCode" -> updateCodeDb doTest myUrl myPort ethUrl mIniBlk (maybe 100 id mNumBlks) iniDb
      "updateErc20" -> updateErc20Db doTest myUrl myPort ethUrl mIniBlk mNumBlks iniDb
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
  let rLogs = traceValueTxLogs traceTx
  print rLogs
  print $ traceTxTree True rLogs
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

getLastBlkNumSucc f myCon mIniBlk = maybe (maybe 1 (+1) <$> f myCon) return mIniBlk

insertBlocksDb doTest mIniBlk numBlks ethUrl myCon doPar = do
  iniBlk <- getLastBlkNumSucc dbSelectLatestBlockNum myCon mIniBlk
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
        print "1 --------------------------------------------------------"
        mapM_ (print . tl2tup) $ map traceLogFromJSON tls
        print "2 --------------------------------------------------------"
        putStrLn $ drawForest $ map (fmap (show . tl2tup)) $ snd $ traceTxTree True tls
        ) (getTxs blk)
      let txs = sort $ nub $ map (\(MyTouchedAccount _ txIdx _) -> txIdx) rDacc
      let txAddrs = map (\txIdx -> (txIdx,map (\(MyTouchedAccount _ _ to) -> to) $ filter (\(MyTouchedAccount _ txIdx' _) -> txIdx == txIdx') rDacc)) txs
      rDacc' <- mapM (\(txIdx,addrs) -> map (MyTouchedAccount blkNum txIdx)
                                     <$> deadAccounts ethUrl myCon blkNum txIdx addrs
                  ) txAddrs
      mapM_ print rDacc'
    else
      ignoreCtrlC $ withTransaction myCon $ do
        -- insertar bloque
        dbInsertMyTx myCon mtx
        -- transacciones
        print "TXS"
        dbInsertTxs myCon rTx
        -- contract creations
        print "NEWS"
        dbInsertContractCreations myCon rNew
        -- msg calls
        print "CALLS"
        dbInsertMsgCalls myCon rCall
        -- internal transactions
        print "INTERNAL TXS"
        dbInsertInternalTxs myCon rItx
        -- dead accounts
        print "DEAD ACCOUNTS"
        insertMyTouchedAccountsDb ethUrl myCon blkNum rDacc
        -- contracts code
        print "CONTRACTS CODE"
        let mtxsNew = joinNews (rNew ++ filter (\(MyInternalTx _ _ _ _ _ op) -> op == OpCREATE) rItx)
        mapM_ (mapM_ (insertBlockContractCodeDb myCon ethUrl)) mtxsNew
        -- new ERC20s y logs ERC20
        print "ERC20s"
        (newErc20s,newNoErc20s,hErc20logs) <- getErc20Db myCon ethUrl blkNum blkNum
        insertErc20Db myCon newErc20s newNoErc20s hErc20logs
  where
    tl2tup tl = (traceLogDepth tl, traceLogOp tl)

getErc20Addrs myCon mtxs = do
  let addrsTo = nub $ map getCallAddrTo $ filter isCallMtx mtxs
  map (\(_,_,addr,_,_,_,_) -> addr) <$> dbSelectErc20Addrs myCon addrsTo
  where
    isCallMtx MyMsgCall{} = True
    isCallMtx (MyInternalTx _ _ _ _ _ op) = case op of
      OpCALL -> True
      OpCALLCODE -> True
      OpDELEGATECALL -> True
      _ -> False
    getCallAddrTo (MyMsgCall _ _ _ toA) = toA
    getCallAddrTo (MyInternalTx _ _ _ _ addr _) = addr

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
                    else traceTxTree True $ traceValueTxLogs txTrace
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
                        then getDbInternalTxs blkNum txIdx cAddr tls
                        else ([],[])
  return (mtx1,mtx2,mdas2,(mtxs3,mdas3))

traceLogFromJSON :: Value -> RpcTraceLog
traceLogFromJSON = (\(Success a) -> a) . fromJSON

traceTxTree :: Bool -> [Value] -> (Word64,[Tree RpcTraceLog])
traceTxTree doReduce = traceTxVal ([],[[]]) 1
  where
    traceTxVal (ops,forest:[]) _ [] = (traceMaskOps ops,reverse forest)
    traceTxVal ret depth (v:vs) = traceTx ret depth (traceLogFromJSON v) v vs
    traceTx (ops,forest:calls) depth t@(RpcTraceLog d me _ _ _ op _ _ _) v vs
      | depth == d = let op' = fromText op
                         forest' = if not doReduce || op' `elem` reducedOps
                                     then (Node t []:forest)
                                     else forest
                     in traceTxVal (op':ops,forest':calls) d vs
      | depth < d = traceTxVal (fromText op:ops,[Node t []]:forest:calls) d vs
      | depth > d = let (Node tp _:forest') = head calls
                        calls' = tail calls
                        vs' = if depth - 1 == d
                                then vs
                                else v:vs
                        res = (Node t []:Node tp (reverse forest):forest'):calls'
                    in traceTxVal (fromText op:ops,res) (depth - 1) vs'
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
    getDbInternalTx (idx,from,to,op) =
      MyInternalTx blkNum txIdx idx from to op
    getItxs idx addr forest = foldl getItx ([],[],idx,addr,forest) forest
    getItx (r,tchs,idx,addr,_:ts) t =
      let (RpcTraceLog _ _ _ _ _ opStr _ mstack _) = rootLabel t
          op = fromText opStr
          (addr',itx,tch) = case op of
            OpCALL ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (to',Just (idx,addr,to',op),tch)
            OpCALLCODE ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',op),tch)
            OpDELEGATECALL ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',op),tch)
            OpSTATICCALL ->
              let (_:to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (to',Just (idx,addr,to',op),tch)
            {- OpREVERT -> -}
            OpCREATE ->
              let t' = rootLabel $ head ts
                  (to:_) = getStack (traceLogStack t')
                  (to',_) = stackAddr' to
              in (to',Just (idx,addr,to',op),Nothing)
            OpSELFDESTRUCT ->
              let (to:_) = getStack mstack
                  (to',tch) = stackAddr' to
              in (addr,Just (idx,addr,to',op),tch)
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

insertMyTouchedAccountsDb ethUrl myCon blkNum rDacc = do
  let proto = ethProto publicEthProtoCfg blkNum
  when (proto `elem` enumFrom SpuriousDragon) $ do
    let rDacc1 = nubBy (\(MyTouchedAccount _ _ to1) (MyTouchedAccount _ _ to2) -> to1 == to2) rDacc
    let txIdxs = sort $ nub $ map (\(MyTouchedAccount _ txIdx _) -> txIdx) rDacc1
    let txIdxAddrs = map (\txIdx -> (txIdx,nub $ map (\(MyTouchedAccount _ _ to) -> to) $ filter (\(MyTouchedAccount _ txIdx' _) -> txIdx == txIdx') rDacc1)) txIdxs
    rDacc2 <- concat <$> mapM (\(txIdx,addrs) ->
                if null addrs
                  then return []
                  else map (MyTouchedAccount blkNum txIdx)
                   <$> deadAccounts ethUrl myCon blkNum txIdx addrs
                ) txIdxAddrs
    unless (null rDacc2) $ dbInsertDeadAccounts myCon rDacc2

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
  addrs4 <- do
    addrs4a <- dbSelectMsgCallsFroms myCon blkNum txIdx addrs3
    return $ filter (\addr -> addr `notElem` addrs4a) addrs3
  addrs5a <- dbSelectContractCreationFroms myCon blkNum txIdx addrs4
  return $ filter (\addr -> addr `notElem` addrs5a) addrs4

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
  iniBlk <- getLastBlkNumSucc dbSelectContractCodeLastBlkNum myCon mIniBlk
  txsNew <- dbSelectContractCreationFromBlkNum myCon iniBlk (iniBlk+numBlks)
  itxsNew <- dbSelectInternalTxFromBlkNum myCon iniBlk (iniBlk+numBlks) OpCREATE
  let blksMtxsNew = joinNews (txsNew ++ itxsNew)
  mapM_ ( ignoreCtrlC
        . withTransaction myCon
        . mapM_ (insertBlockContractCodeDb myCon ethUrl)
    ) blksMtxsNew
  close myCon

joinNews = groupBy eqMtxBlkNum . sortBy (comparing mtxIdx)
  where
    eqMtxBlkNumAddr mtx1 mtx2 = mtxBlkNumAddr mtx1 == mtxBlkNumAddr mtx2
    mtxBlkNumAddr mtx = case mtx of
      MyContractCreation blkNum _ _ addr -> (blkNum,addr)
      MyInternalTx blkNum _ _ _ addr _ -> (blkNum,addr)
      _ -> error $ "mtxBlkNumAddr: " ++ show mtx
    eqMtxBlkNum mtx1 mtx2 = mtxBlkNum mtx1 == mtxBlkNum mtx2
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
    dbInsertContractCode myCon blkNum txIdx addr mBzzr0 code

mtxGetCodeInfo mtx = case mtx of
  MyContractCreation blkNum txIdx _ addr -> (blkNum,txIdx,addr)
  MyInternalTx blkNum txIdx _ _ addr _ -> (blkNum,txIdx,addr)

updateErc20Db doTest myUrl myPort ethUrl mIniBlk mNumBlks iniDb = do
  (greet,myCon) <- connectDetail (defConInfo myUrl myPort)
  printErr greet
  when iniDb $ dbCreateTableErc20s myCon
  -- continuar a partir del último bloque procesado
  iniBlk <- getLastBlkNumSucc dbSelectErc20LogLastBlkNum myCon mIniBlk
  lstBlk <- maybe (maybe 1 id <$> dbSelectLatestBlockNum myCon) (return . (+(iniBlk-1))) mNumBlks
  (newErc20s,newNoErc20s,hLogs) <- getErc20Db myCon ethUrl iniBlk lstBlk
  if doTest
    then do
      mapM_ print newNoErc20s
      mapM_ print newErc20s
      mapM_ (mapM_ print) $ HM.elems hLogs
    else ignoreCtrlC $ withTransaction myCon $ do
      insertErc20Db myCon newErc20s newNoErc20s hLogs
  close myCon

insertErc20Db myCon newErc20s newNoErc20s hErc20logs = do
  insertTokenAppsDb myCon newErc20s
  insertTokenAppsDb myCon newNoErc20s
  mapM_ (dbInsertErc20Logs myCon . snd) $ HM.toList hErc20logs

insertTokenAppsDb myCon =
  mapM_ (\(blkNum,txIdx,addr,isErc20,mName,mSymbol,mDecimals) ->
    dbInsertErc20 myCon blkNum txIdx addr isErc20 mName mSymbol mDecimals
    )

-- | El método para identificar los contracts que implementan un token es
-- buscarlos en los events ERC20 de los logs:
--  1 - obtener los events ERC20 de los logs
--  2 - identificar las direcciones de nuevos contracts
--  3 - asumir que los contracts pueden no seguir la especificación ERC20/EIP20
--      rigurosamente
--  4 - obtener información del token de los nuevos contracts
getErc20Db myCon ethUrl iniBlk lstBlk = do
  -- seleccionar (RPC) logs ERC20 desde iniBlk hasta lstBlk
  hErc20logs <- spanLogsByAddr <$> getErc20Logs ethUrl iniBlk lstBlk
  let logAddrs = sort $ HM.keys hErc20logs
  -- seleccionar (DB) ERC20s de los logs anteriores
  erc20s <- dbSelectErc20Addrs myCon logAddrs
  let erc20sAddrs = map erc20GetAddr erc20s
  -- obtener las direcciones no presentes en la DB
  let newAddrs = filterNewAddrs erc20sAddrs logAddrs
  -- seleccionar (DB) contractsCode de las nuevas direcciones
  contCodes <- dbSelectContractCodeAddrs myCon newAddrs
  -- filtrar los contract que tienen las funciones obligatorias
  let erc20Sels = map (stripHex . toHex . erc20Selector)
                $ HM.elems
                $ HM.filter erc20IsFunction
                $ HM.filter erc20IsRequired erc20Info
  let (erc20Codes,noErc20Codes) = partition (\(_,_,_,_,code) -> all (\sel -> sel `T.isInfixOf` code) erc20Sels) contCodes
  newErc20s <- getTokenInfo' True erc20Codes
  newNoErc20s <- getTokenInfo' False noErc20Codes
  return (newErc20s,newNoErc20s,hErc20logs)
  where
    getTokenInfo' isErc20 codes = sortByBlkNumTxIdx
                              <$> getTokenInfo ethUrl isErc20 codes
    sortByBlkNumTxIdx = sortBy (\(b1,i1,_,_,_,_,_) (b2,i2,_,_,_,_,_) ->
      let c1 = compare b1 b2
          c2 = compare i1 i2
      in if c1==EQ then c2 else c1)

filterNewAddrs addrs = filter (\addr -> addr `notElem` addrs)

spanLogsByAddr = spanMtxsByAddr erc20LogGetAddr

contractCodeGetAddr (_,_,addr,_,_) = addr
erc20GetAddr (_,_,addr,_,_,_,_) = addr
erc20LogGetAddr (MyErc20Log _ _ addr _ _ _ _) = addr

getAddrsErc20Logs :: String -> BlockNum -> BlockNum
                  -> [HexEthAddr] -> IO [MysqlTx]
getAddrsErc20Logs ethUrl iniBlk lstBlk addrs = do
  getMyErc20Logs <$> getAddrLogs ethUrl iniBlk lstBlk addrs

getErc20Logs :: String -> BlockNum -> BlockNum -> IO [MysqlTx]
getErc20Logs ethUrl iniBlk lstBlk = do
  getMyErc20Logs <$> getTopic0Logs ethUrl iniBlk lstBlk

-- | Decodifica los events ERC20 de los logs
getMyErc20Logs :: [RpcEthLog] -> [MysqlTx]
getMyErc20Logs logs =
  map getMyErc20Log
    $ map (\(l,edl) -> (l,fromRight "getMyErc20Logs" edl))
    $ filter (isRight . snd)
    $ map (\(l,medl) -> (l,fromJust medl))
    $ filter (isJust . snd)
    $ map (\(l1,l2) -> (l1,eip20interface_decode_log l2))
    $ zip logs logs
  where
    getMyErc20Log ((RpcEthLog _ _ mTxIdx _ _ mBlkNum addr _ _),ev) =
      let (transfer,fromA,toA,amount) = getEvtInfo ev
          blkNum = fromJust mBlkNum
          txIdx = fromJust mTxIdx
      in MyErc20Log blkNum txIdx addr transfer fromA toA amount
    getEvtInfo ev = case ev of
      EIP20Interface_Transfer (fromA,toA,amount) -> (True,fromA,toA,amount)
      EIP20Interface_Approval (fromA,toA,amount) -> (False,fromA,toA,amount)

getAddrLogs :: String -> BlockNum -> BlockNum
            -> [HexEthAddr] -> IO [RpcEthLog]
getAddrLogs ethUrl iniBlk lstBlk addrs =
  map (\(EthFilterLog l) -> l) . fromRight "getLogs"
    <$> runWeb3 False ethUrl
          (eth_getLogs $ RpcEthFilter
                           (Just $ RPBNum iniBlk)
                           (Just $ RPBNum lstBlk)
                           (Just addrs)
                           Nothing)

-- | Obtiene los logs de los events ERC20 en el rango de bloques. Los events
-- ERC20 (Transfer y Approval) tienen dos argumentos `indexed`
getTopic0Logs :: String -> BlockNum -> BlockNum -> IO [RpcEthLog]
getTopic0Logs ethUrl iniBlk lstBlk =
  map (\(EthFilterLog l) -> l) . fromRight "getTopic0Logs"
    <$> runWeb3 False ethUrl
          (eth_getLogs $ RpcEthFilter
                           (Just $ RPBNum iniBlk)
                           (Just $ RPBNum lstBlk)
                           Nothing
                           (Just [erc20Topics0,topicNull,topicNull]))
  where
    erc20Topics0 = EthFilterOrTopics
                 $ map (EthFilterTopic . toHex . keccak256 . toCanonical)
                 $ map (\(IEvent evt) -> evt)
                 $ HM.elems
                 $ HM.map erc20Interface
                 $ HM.filter erc20IsEvent erc20Info

getTokenInfo ethUrl isErc20 = mapM (\(blkNum,txIdx,addr,_,code) -> do
  (mName,mSymbol,mDecimals) <- erc20GetTokenInfo ethUrl addr code
  return (blkNum,txIdx,addr,isErc20,mName,mSymbol,mDecimals)
  )

-- | Obtiene la información del token si es posible. Intenta obtener el
-- `name`, `symbol` y `decimals` llamando a las funciones, si están presentes
-- en el código binario del contract y no falla la llamada.
erc20GetTokenInfo :: String -> HexEthAddr -> Text
                  -> IO (Maybe Text, Maybe Text, Maybe Uint8)
erc20GetTokenInfo ethUrl addr code = do
  name <- erc20_call "name" eip20interface_name_call
  symbol <- erc20_call "symbol" eip20interface_symbol_call
  decimals <- erc20_call "decimals" eip20interface_decimals_call
  return (name, symbol, decimals)
  where
    erc20_call :: (FromJSON a)
               => Text
               -> ( HexEthAddr -> HexEthAddr
                 -> Web3T Request (LoggingT IO) (Either Text a))
               -> IO (Maybe a)
    erc20_call nom f = do
      let sel = stripHex $ toHex $ erc20Selector $ erc20Info HM.! nom
      if sel `T.isInfixOf` code
        then either (const Nothing) (either (const Nothing) Just)
                <$> runWeb3 False ethUrl (f addr addr)
        else return Nothing

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

