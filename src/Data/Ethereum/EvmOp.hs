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
  , toOpcode
  ) where

import Data.Monoid ((<>))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word8)

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
  deriving (Eq,Show)

fromText :: Text -> EvmOp
fromText op = case op of
  "STOP" -> OpSTOP
  "ADD" -> OpADD
  "MUL" -> OpMUL
  "SUB" -> OpSUB
  "DIV" -> OpDIV
  "SDIV" -> OpSDIV
  "MOD" -> OpMOD
  "SMOD" -> OpSMOD
  "ADDMOD" -> OpADDMOD
  "MULMOD" -> OpMULMOD
  "EXP" -> OpEXP
  "SIGNEXTEND" -> OpSIGNEXTEND
  "LT" -> OpLT
  "GT" -> OpGT
  "SLT" -> OpSLT
  "SGT" -> OpSGT
  "EQ" -> OpEQ
  "ISZERO" -> OpISZERO
  "AND" -> OpAND
  "OR" -> OpOR
  "XOR" -> OpXOR
  "NOT" -> OpNOT
  "BYTE" -> OpBYTE
  "SHA3" -> OpSHA3
  "ADDRESS" -> OpADDRESS
  "BALANCE" -> OpBALANCE
  "ORIGIN" -> OpORIGIN
  "CALLER" -> OpCALLER
  "CALLVALUE" -> OpCALLVALUE
  "CALLDATALOAD" -> OpCALLDATALOAD
  "CALLDATASIZE" -> OpCALLDATASIZE
  "CALLDATACOPY" -> OpCALLDATACOPY
  "CODESIZE" -> OpCODESIZE
  "CODECOPY" -> OpCODECOPY
  "GASPRICE" -> OpGASPRICE
  "EXTCODESIZE" -> OpEXTCODESIZE
  "EXTCODECOPY" -> OpEXTCODECOPY
  "RETURNDATASIZE" -> OpRETURNDATASIZE
  "RETURNDATACOPY" -> OpRETURNDATACOPY
  "BLOCKHASH" -> OpBLOCKHASH
  "COINBASE" -> OpCOINBASE
  "TIMESTAMP" -> OpTIMESTAMP
  "NUMBER" -> OpNUMBER
  "DIFFICULTY" -> OpDIFFICULTY
  "GASLIMIT" -> OpGASLIMIT
  "POP" -> OpPOP
  "MLOAD" -> OpMLOAD
  "MSTORE" -> OpMSTORE
  "MSTORE8" -> OpMSTORE8
  "SLOAD" -> OpSLOAD
  "SSTORE" -> OpSSTORE
  "JUMP" -> OpJUMP
  "JUMPI" -> OpJUMPI
  "PC" -> OpPC
  "MSIZE" -> OpMSIZE
  "GAS" -> OpGAS
  "JUMPDEST" -> OpJUMPDEST
  "PUSH1" -> OpPUSH 1
  "PUSH2" -> OpPUSH 2
  "PUSH3" -> OpPUSH 3
  "PUSH4" -> OpPUSH 4
  "PUSH5" -> OpPUSH 5
  "PUSH6" -> OpPUSH 6
  "PUSH7" -> OpPUSH 7
  "PUSH8" -> OpPUSH 8
  "PUSH9" -> OpPUSH 9
  "PUSH10" -> OpPUSH 10
  "PUSH11" -> OpPUSH 11
  "PUSH12" -> OpPUSH 12
  "PUSH13" -> OpPUSH 13
  "PUSH14" -> OpPUSH 14
  "PUSH15" -> OpPUSH 15
  "PUSH16" -> OpPUSH 16
  "PUSH17" -> OpPUSH 17
  "PUSH18" -> OpPUSH 18
  "PUSH19" -> OpPUSH 19
  "PUSH20" -> OpPUSH 20
  "PUSH21" -> OpPUSH 21
  "PUSH22" -> OpPUSH 22
  "PUSH23" -> OpPUSH 23
  "PUSH24" -> OpPUSH 24
  "PUSH25" -> OpPUSH 25
  "PUSH26" -> OpPUSH 26
  "PUSH27" -> OpPUSH 27
  "PUSH28" -> OpPUSH 28
  "PUSH29" -> OpPUSH 29
  "PUSH30" -> OpPUSH 30
  "PUSH31" -> OpPUSH 31
  "PUSH32" -> OpPUSH 32
  "DUP1" -> OpDUP 1
  "DUP2" -> OpDUP 2
  "DUP3" -> OpDUP 3
  "DUP4" -> OpDUP 4
  "DUP5" -> OpDUP 5
  "DUP6" -> OpDUP 6
  "DUP7" -> OpDUP 7
  "DUP8" -> OpDUP 8
  "DUP9" -> OpDUP 9
  "DUP10" -> OpDUP 10
  "DUP11" -> OpDUP 11
  "DUP12" -> OpDUP 12
  "DUP13" -> OpDUP 13
  "DUP14" -> OpDUP 14
  "DUP15" -> OpDUP 15
  "DUP16" -> OpDUP 16
  "SWAP1" -> OpSWAP 1
  "SWAP2" -> OpSWAP 2
  "SWAP3" -> OpSWAP 3
  "SWAP4" -> OpSWAP 4
  "SWAP5" -> OpSWAP 5
  "SWAP6" -> OpSWAP 6
  "SWAP7" -> OpSWAP 7
  "SWAP8" -> OpSWAP 8
  "SWAP9" -> OpSWAP 9
  "SWAP10" -> OpSWAP 10
  "SWAP11" -> OpSWAP 11
  "SWAP12" -> OpSWAP 12
  "SWAP13" -> OpSWAP 13
  "SWAP14" -> OpSWAP 14
  "SWAP15" -> OpSWAP 15
  "SWAP16" -> OpSWAP 16
  "LOG0" -> OpLOG 0
  "LOG1" -> OpLOG 1
  "LOG2" -> OpLOG 2
  "LOG3" -> OpLOG 3
  "LOG4" -> OpLOG 4
  "CREATE" -> OpCREATE
  "CALL" -> OpCALL
  "CALLCODE" -> OpCALLCODE
  "RETURN" -> OpRETURN
  "DELEGATECALL" -> OpDELEGATECALL
  "STATICCALL" -> OpSTATICCALL
  "REVERT" -> OpREVERT
  "INVALID" -> OpINVALID
  "SELFDESTRUCT" -> OpSELFDESTRUCT
  "SUICIDE" -> OpSELFDESTRUCT

toText :: EvmOp -> Text
toText op = case op of
  OpSTOP -> "STOP"
  OpADD -> "ADD"
  OpMUL -> "MUL"
  OpSUB -> "SUB"
  OpDIV -> "DIV"
  OpSDIV -> "SDIV"
  OpMOD -> "MOD"
  OpSMOD -> "SMOD"
  OpADDMOD -> "ADDMOD"
  OpMULMOD -> "MULMOD"
  OpEXP -> "EXP"
  OpSIGNEXTEND -> "SIGNEXTEND"
  OpLT -> "LT"
  OpGT -> "GT"
  OpSLT -> "SLT"
  OpSGT -> "SGT"
  OpEQ -> "EQ"
  OpISZERO -> "ISZERO"
  OpAND -> "AND"
  OpOR -> "OR"
  OpXOR -> "XOR"
  OpNOT -> "NOT"
  OpBYTE -> "BYTE"
  OpSHA3 -> "SHA3"
  OpADDRESS -> "ADDRESS"
  OpBALANCE -> "BALANCE"
  OpORIGIN -> "ORIGIN"
  OpCALLER -> "CALLER"
  OpCALLVALUE -> "CALLVALUE"
  OpCALLDATALOAD -> "CALLDATALOAD"
  OpCALLDATASIZE -> "CALLDATASIZE"
  OpCALLDATACOPY -> "CALLDATACOPY"
  OpCODESIZE -> "CODESIZE"
  OpCODECOPY -> "CODECOPY"
  OpGASPRICE -> "GASPRICE"
  OpEXTCODESIZE -> "EXTCODESIZE"
  OpEXTCODECOPY -> "EXTCODECOPY"
  OpRETURNDATASIZE -> "RETURNDATASIZE"
  OpRETURNDATACOPY -> "RETURNDATACOPY"
  OpBLOCKHASH -> "BLOCKHASH"
  OpCOINBASE -> "COINBASE"
  OpTIMESTAMP -> "TIMESTAMP"
  OpNUMBER -> "NUMBER"
  OpDIFFICULTY -> "DIFFICULTY"
  OpGASLIMIT -> "GASLIMIT"
  OpPOP -> "POP"
  OpMLOAD -> "MLOAD"
  OpMSTORE -> "MSTORE"
  OpMSTORE8 -> "MSTORE8"
  OpSLOAD -> "SLOAD"
  OpSSTORE -> "SSTORE"
  OpJUMP -> "JUMP"
  OpJUMPI -> "JUMPI"
  OpPC -> "PC"
  OpMSIZE -> "MSIZE"
  OpGAS -> "GAS"
  OpJUMPDEST -> "JUMPDEST"
  OpPUSH n -> opIdxStr "PUSH" n
  OpDUP n -> opIdxStr "DUP" n
  OpSWAP n -> opIdxStr "SWAP" n
  OpLOG n -> opIdxStr "LOG" n
  OpCREATE -> "CREATE"
  OpCALL -> "CALL"
  OpCALLCODE -> "CALLCODE"
  OpRETURN -> "RETURN"
  OpDELEGATECALL -> "DELEGATECALL"
  OpSTATICCALL -> "STATICCALL"
  OpREVERT -> "REVERT"
  OpINVALID -> "INVALID"
  OpSELFDESTRUCT -> "SELFDESTRUCT"
  where
    opIdxStr op idx = op <> T.pack (show idx)

toOpcode :: EvmOp -> Word8
toOpcode op = case op of
  OpSTOP -> 0x0
  OpADD -> 0x1
  OpMUL -> 0x2
  OpSUB -> 0x3
  OpDIV -> 0x4
  OpSDIV -> 0x5
  OpMOD -> 0x6
  OpSMOD -> 0x7
  OpADDMOD -> 0x8
  OpMULMOD -> 0x9
  OpEXP -> 0xa
  OpSIGNEXTEND -> 0xb
  OpLT -> 0x10
  OpGT -> 0x11
  OpSLT -> 0x12
  OpSGT -> 0x13
  OpEQ -> 0x14
  OpISZERO -> 0x15
  OpAND -> 0x16
  OpOR -> 0x17
  OpXOR -> 0x18
  OpNOT -> 0x19
  OpBYTE -> 0x1a
  OpSHA3 -> 0x20
  OpADDRESS -> 0x30
  OpBALANCE -> 0x31
  OpORIGIN -> 0x32
  OpCALLER -> 0x33
  OpCALLVALUE -> 0x34
  OpCALLDATALOAD -> 0x35
  OpCALLDATASIZE -> 0x36
  OpCALLDATACOPY -> 0x37
  OpCODESIZE -> 0x38
  OpCODECOPY -> 0x39
  OpGASPRICE -> 0x3a
  OpEXTCODESIZE -> 0x3b
  OpEXTCODECOPY -> 0x3c
  OpRETURNDATASIZE -> 0x3d
  OpRETURNDATACOPY -> 0x3e
  OpBLOCKHASH -> 0x40
  OpCOINBASE -> 0x41
  OpTIMESTAMP -> 0x42
  OpNUMBER -> 0x43
  OpDIFFICULTY -> 0x44
  OpGASLIMIT -> 0x45
  OpPOP -> 0x50
  OpMLOAD -> 0x51
  OpMSTORE -> 0x52
  OpMSTORE8 -> 0x53
  OpSLOAD -> 0x54
  OpSSTORE -> 0x55
  OpJUMP -> 0x56
  OpJUMPI -> 0x57
  OpPC -> 0x58
  OpMSIZE -> 0x59
  OpGAS -> 0x5a
  OpJUMPDEST -> 0x5b
  OpPUSH n -> 0x60 + n - 1
  OpDUP n -> 0x80 + n - 1
  OpSWAP n -> 0x90 + n - 1
  OpLOG n -> 0xa0 + n
  OpCREATE -> 0xf0
  OpCALL -> 0xf1
  OpCALLCODE -> 0xf2
  OpRETURN -> 0xf3
  OpDELEGATECALL -> 0xf4
  OpSTATICCALL -> 0xfa
  OpREVERT -> 0xfd
  OpINVALID -> 0xfe
  OpSELFDESTRUCT -> 0xff


