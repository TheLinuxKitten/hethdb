{-# LANGUAGE OverloadedStrings #-}

--------------------------------------------------------------------------
--
-- Copyright: (c) Javier López Durá
-- License: BSD3
--
-- Maintainer: Javier López Durá <linux.kitten@gmail.com>
--
--------------------------------------------------------------------------

module Data.Ethereum.EvmDisasm
  ( parseEvmCode
  ) where

import Control.Applicative ((<|>))
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.Monoid ((<>))
import qualified Data.Text as T
import Data.Text (Text)
import Network.Web3.HexText
import Network.Web3.Types (HexData)
import qualified Text.Parsec as P
import qualified Text.Parsec.Char as P
import qualified Text.Parsec.Text as P

parseOpcode :: P.Parser Text
parseOpcode = do
  pos <- (`div` 2) . P.sourceColumn <$> P.getPosition
  [op] <- BS.unpack . hex2bs <$> parseByteCode
  case op of
    0x00 -> returnPos pos "STOP"
    0x01 -> returnPos pos "ADD"
    0x02 -> returnPos pos "MUL"
    0x03 -> returnPos pos "SUB"
    0x04 -> returnPos pos "DIV"
    0x05 -> returnPos pos "SDIV"
    0x06 -> returnPos pos "MOD"
    0x07 -> returnPos pos "SMOD"
    0x08 -> returnPos pos "ADDMOD"
    0x09 -> returnPos pos "MULMOD"
    0x0a -> returnPos pos "EXP"
    0x0b -> returnPos pos "SIGNEXTEND"

    0x10 -> returnPos pos "LT"
    0x11 -> returnPos pos "GT"
    0x12 -> returnPos pos "SLT"
    0x13 -> returnPos pos "SGT"
    0x14 -> returnPos pos "EQ"
    0x15 -> returnPos pos "ISZERO"
    0x16 -> returnPos pos "AND"
    0x17 -> returnPos pos "OR"
    0x18 -> returnPos pos "XOR"
    0x19 -> returnPos pos "NOT"
    0x1a -> returnPos pos "BYTE"

    0x20 -> returnPos pos "SHA3"

    0x30 -> returnPos pos "ADDRESS"
    0x31 -> returnPos pos "BALANCE"
    0x32 -> returnPos pos "ORIGIN"
    0x33 -> returnPos pos "CALLER"
    0x34 -> returnPos pos "CALLVALUE"
    0x35 -> returnPos pos "CALLDATALOAD"
    0x36 -> returnPos pos "CALLDATASIZE"
    0x37 -> returnPos pos "CALLDATACOPY"
    0x38 -> returnPos pos "CODEZISE"
    0x39 -> returnPos pos "CODECOPY"
    0x3a -> returnPos pos "GASPRICE"
    0x3b -> returnPos pos "EXTCODESIZE"
    0x3c -> returnPos pos "EXTCODECOPY"
    0x3d -> returnPos pos "RETURNDATASIZE"
    0x3e -> returnPos pos "RETURNDATACOPY"

    0x40 -> returnPos pos "BLOCKHASH"
    0x41 -> returnPos pos "COINBASE"
    0x42 -> returnPos pos "TIMESTAMP"
    0x43 -> returnPos pos "NUMBER"
    0x44 -> returnPos pos "DIFFICULTY"
    0x45 -> returnPos pos "GASLIMIT"

    0x50 -> returnPos pos "POP"
    0x51 -> returnPos pos "MLOAD"
    0x52 -> returnPos pos "MSTORE"
    0x53 -> returnPos pos "MSTORE8"
    0x54 -> returnPos pos "SLOAD"
    0x55 -> returnPos pos "SSTORE"
    0x56 -> returnPos pos "JUMP"
    0x57 -> returnPos pos "JUMPI"
    0x58 -> returnPos pos "PC"
    0x59 -> returnPos pos "MSIZE"
    0x5a -> returnPos pos "GAS"
    0x5b -> returnPos pos "JUMPDEST"

    0x60 -> parsePush pos 1
    0x61 -> parsePush pos 2
    0x62 -> parsePush pos 3
    0x63 -> parsePush pos 4
    0x64 -> parsePush pos 5
    0x65 -> parsePush pos 6
    0x66 -> parsePush pos 7
    0x67 -> parsePush pos 8
    0x68 -> parsePush pos 9
    0x69 -> parsePush pos 10
    0x6a -> parsePush pos 11
    0x6b -> parsePush pos 12
    0x6c -> parsePush pos 13
    0x6d -> parsePush pos 14
    0x6e -> parsePush pos 15
    0x6f -> parsePush pos 16
    0x70 -> parsePush pos 17
    0x71 -> parsePush pos 18
    0x72 -> parsePush pos 19
    0x73 -> parsePush pos 20
    0x74 -> parsePush pos 21
    0x75 -> parsePush pos 22
    0x76 -> parsePush pos 23
    0x77 -> parsePush pos 24
    0x78 -> parsePush pos 25
    0x79 -> parsePush pos 26
    0x7a -> parsePush pos 27
    0x7b -> parsePush pos 28
    0x7c -> parsePush pos 29
    0x7d -> parsePush pos 30
    0x7e -> parsePush pos 31
    0x7f -> parsePush pos 32

    0x80 -> returnPos pos "DUP1"
    0x81 -> returnPos pos "DUP2"
    0x82 -> returnPos pos "DUP3"
    0x83 -> returnPos pos "DUP4"
    0x84 -> returnPos pos "DUP5"
    0x85 -> returnPos pos "DUP6"
    0x86 -> returnPos pos "DUP7"
    0x87 -> returnPos pos "DUP8"
    0x88 -> returnPos pos "DUP9"
    0x89 -> returnPos pos "DUP10"
    0x8a -> returnPos pos "DUP11"
    0x8b -> returnPos pos "DUP12"
    0x8c -> returnPos pos "DUP13"
    0x8d -> returnPos pos "DUP14"
    0x8e -> returnPos pos "DUP15"
    0x8f -> returnPos pos "DUP16"

    0x90 -> returnPos pos "SWAP1"
    0x91 -> returnPos pos "SWAP2"
    0x92 -> returnPos pos "SWAP3"
    0x93 -> returnPos pos "SWAP4"
    0x94 -> returnPos pos "SWAP5"
    0x95 -> returnPos pos "SWAP6"
    0x96 -> returnPos pos "SWAP7"
    0x97 -> returnPos pos "SWAP8"
    0x98 -> returnPos pos "SWAP9"
    0x99 -> returnPos pos "SWAP10"
    0x9a -> returnPos pos "SWAP11"
    0x9b -> returnPos pos "SWAP12"
    0x9c -> returnPos pos "SWAP13"
    0x9d -> returnPos pos "SWAP14"
    0x9e -> returnPos pos "SWAP15"
    0x9f -> returnPos pos "SWAP16"

    0xa0 -> returnPos pos "LOG0"
    0xa1 -> returnPos pos "LOG1"
    0xa2 -> returnPos pos "LOG2"
    0xa3 -> returnPos pos "LOG3"
    0xa4 -> returnPos pos "LOG4"

    0xf0 -> returnPos pos "CREATE"
    0xf1 -> returnPos pos "CALL"
    0xf2 -> returnPos pos "CALLCODE"
    0xf3 -> returnPos pos "RETURN"
    0xf4 -> returnPos pos "DELEGATECALL"
    0xfa -> returnPos pos "STATICCALL"
    0xfd -> returnPos pos "REVERT"
    0xfe -> returnPos pos "INVALID"
    0xff -> returnPos pos "SUICIDE"
  where
    parseByteCode = T.pack <$> P.count 2 P.hexDigit
    parsePush pos numBytes = do
      codeBytes <- joinHex . T.concat <$> P.count numBytes parseByteCode'
      returnPos pos $ "PUSH" <> T.pack (show numBytes) <> " " <> codeBytes
    parseByteCode' = parseByteCode <|> return "00"
    returnPos pos t = return $ T.pack (show pos) <> " " <> t

parseEvmCode :: HexData -> Either Text [Text]
parseEvmCode = either (Left . T.pack . show) Right
             . P.parse (P.many parseOpcode <* P.eof) ""
             . stripHex

