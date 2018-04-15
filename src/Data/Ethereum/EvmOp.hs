{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

--------------------------------------------------------------------------
--
-- Copyright: (c) Javier López Durá
-- License: BSD3
--
-- Maintainer: Javier López Durá <linux.kitten@gmail.com>
--
--------------------------------------------------------------------------

module Data.Ethereum.EvmOp
  ( EvmOp(..)
  , fromText
  , toText
  , fromOpcode
  , toOpcode
  ) where

import Data.Hashable
import qualified Data.HashMap.Lazy as HM
import Data.Monoid ((<>))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word8)
import GHC.Generics (Generic)

data EvmOp =
    OpSTOP
  | OpADD
  | OpMUL
  | OpSUB
  | OpDIV
  | OpSDIV
  | OpMOD
  | OpSMOD
  | OpADDMOD
  | OpMULMOD
  | OpEXP
  | OpSIGNEXTEND
  | OpLT
  | OpGT
  | OpSLT
  | OpSGT
  | OpEQ
  | OpISZERO
  | OpAND
  | OpOR
  | OpXOR
  | OpNOT
  | OpBYTE
  | OpSHA3
  | OpADDRESS
  | OpBALANCE
  | OpORIGIN
  | OpCALLER
  | OpCALLVALUE
  | OpCALLDATALOAD
  | OpCALLDATASIZE
  | OpCALLDATACOPY
  | OpCODESIZE
  | OpCODECOPY
  | OpGASPRICE
  | OpEXTCODESIZE
  | OpEXTCODECOPY
  | OpRETURNDATASIZE
  | OpRETURNDATACOPY
  | OpBLOCKHASH
  | OpCOINBASE
  | OpTIMESTAMP
  | OpNUMBER
  | OpDIFFICULTY
  | OpGASLIMIT
  | OpPOP
  | OpMLOAD
  | OpMSTORE
  | OpMSTORE8
  | OpSLOAD
  | OpSSTORE
  | OpJUMP
  | OpJUMPI
  | OpPC
  | OpMSIZE
  | OpGAS
  | OpJUMPDEST
  | OpPUSH Word8
  | OpDUP Word8
  | OpSWAP Word8
  | OpLOG Word8
  | OpCREATE
  | OpCALL
  | OpCALLCODE
  | OpRETURN
  | OpDELEGATECALL
  | OpSTATICCALL
  | OpREVERT
  | OpINVALID
  | OpSELFDESTRUCT
  deriving (Eq,Generic,Show)

instance Hashable EvmOp

fromText :: Text -> EvmOp
fromText t = maybe OpINVALID fst' (t `HM.lookup` hmOpNoms)

toText :: EvmOp -> Text
toText op = maybe (toText OpINVALID) thr' (op `HM.lookup` hmOps)

fromOpcode :: Word8 -> EvmOp
fromOpcode opc = maybe OpINVALID fst' (opc `HM.lookup` hmOpCodes)

toOpcode :: EvmOp -> Word8
toOpcode op = maybe (toOpcode OpINVALID) snd' (op `HM.lookup` hmOps)

hmOps = opHm fst'
hmOpCodes = opHm snd'
hmOpNoms = opHm thr'

opHm f = HM.fromList $ map (\opt -> (f opt, opt)) opcodeTable

fst' (a,_,_) = a
snd' (_,b,_) = b
thr' (_,_,c) = c

opcodeTable =
  [ (OpSTOP, 0x0, "STOP")
  , (OpADD, 0x1, "ADD")
  , (OpMUL, 0x2, "MUL")
  , (OpSUB, 0x3, "SUB")
  , (OpDIV, 0x4, "DIV")
  , (OpSDIV, 0x5, "SDIV")
  , (OpMOD, 0x6, "MOD")
  , (OpSMOD, 0x7, "SMOD")
  , (OpADDMOD, 0x8, "ADDMOD")
  , (OpMULMOD, 0x9, "MULMOD")
  , (OpEXP, 0xa, "EXP")
  , (OpSIGNEXTEND, 0xb, "SIGNEXTEND")
  , (OpLT, 0x10, "LT")
  , (OpGT, 0x11, "GT")
  , (OpSLT, 0x12, "SLT")
  , (OpSGT, 0x13, "SGT")
  , (OpEQ, 0x14, "EQ")
  , (OpISZERO, 0x15, "ISZERO")
  , (OpAND, 0x16, "AND")
  , (OpOR, 0x17, "OR")
  , (OpXOR, 0x18, "XOR")
  , (OpNOT, 0x19, "NOT")
  , (OpBYTE, 0x1a, "BYTE")
  , (OpSHA3, 0x20, "SHA3")
  , (OpADDRESS, 0x30, "ADDRESS")
  , (OpBALANCE, 0x31, "BALANCE")
  , (OpORIGIN, 0x32, "ORIGIN")
  , (OpCALLER, 0x33, "CALLER")
  , (OpCALLVALUE, 0x34, "CALLVALUE")
  , (OpCALLDATALOAD, 0x35, "CALLDATALOAD")
  , (OpCALLDATASIZE, 0x36, "CALLDATASIZE")
  , (OpCALLDATACOPY, 0x37, "CALLDATACOPY")
  , (OpCODESIZE, 0x38, "CODESIZE")
  , (OpCODECOPY, 0x39, "CODECOPY")
  , (OpGASPRICE, 0x3a, "GASPRICE")
  , (OpEXTCODESIZE, 0x3b, "EXTCODESIZE")
  , (OpEXTCODECOPY, 0x3c, "EXTCODECOPY")
  , (OpRETURNDATASIZE, 0x3d, "RETURNDATASIZE")
  , (OpRETURNDATACOPY, 0x3e, "RETURNDATACOPY")
  , (OpBLOCKHASH, 0x40, "BLOCKHASH")
  , (OpCOINBASE, 0x41, "COINBASE")
  , (OpTIMESTAMP, 0x42, "TIMESTAMP")
  , (OpNUMBER, 0x43, "NUMBER")
  , (OpDIFFICULTY, 0x44, "DIFFICULTY")
  , (OpGASLIMIT, 0x45, "GASLIMIT")
  , (OpPOP, 0x50, "POP")
  , (OpMLOAD, 0x51, "MLOAD")
  , (OpMSTORE, 0x52, "MSTORE")
  , (OpMSTORE8, 0x53, "MSTORE8")
  , (OpSLOAD, 0x54, "SLOAD")
  , (OpSSTORE, 0x55, "SSTORE")
  , (OpJUMP, 0x56, "JUMP")
  , (OpJUMPI, 0x57, "JUMPI")
  , (OpPC, 0x58, "PC")
  , (OpMSIZE, 0x59, "MSIZE")
  , (OpGAS, 0x5a, "GAS")
  , (OpJUMPDEST, 0x5b, "JUMPDEST")
  ] ++
  map (opIdx OpPUSH 0x60 "PUSH") [1..32] ++
  map (opIdx OpDUP 0x80 "DUP") [1..16] ++
  map (opIdx OpSWAP 0x90 "SWAP") [1..16] ++
  map (opIdx OpLOG (0xa0+1) "LOG") [0..4] ++
  [ (OpCREATE, 0xf0, "CREATE")
  , (OpCALL, 0xf1, "CALL")
  , (OpCALLCODE, 0xf2, "CALLCODE")
  , (OpRETURN, 0xf3, "RETURN")
  , (OpDELEGATECALL, 0xf4, "DELEGATECALL")
  , (OpSTATICCALL, 0xfa, "STATICCALL")
  , (OpREVERT, 0xfd, "REVERT")
  , (OpINVALID, 0xfe, "INVALID")
  , (OpSELFDESTRUCT, 0xff, "SUICIDE")
  , (OpSELFDESTRUCT, 0xff, "SELFDESTRUCT")
  ]
  where
    opIdx op iniOpc str idx = (op idx, iniOpc + idx - 1, str <> T.pack (show idx))


