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
import Data.Ethereum.EvmOp
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
  let opNom = toText (fromOpcode op)
  if op >= 0x60 && op <= 0x7f
    then parsePush pos opNom (fromIntegral op - 0x60 + 1)
    else returnPos pos opNom
  where
    parseByteCode = T.pack <$> P.count 2 P.hexDigit
    parsePush pos opNom numBytes = do
      codeBytes <- joinHex . T.concat <$> P.count numBytes parseByteCode'
      returnPos pos $ opNom <> " " <> codeBytes
    parseByteCode' = parseByteCode <|> return "00"
    returnPos pos t = return $ T.pack (show pos) <> " " <> t

parseEvmCode :: HexData -> Either Text [Text]
parseEvmCode = either (Left . T.pack . show) Right
             . P.parse (P.many parseOpcode <* P.eof) ""
             . stripHex

