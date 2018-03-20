{-# LANGUAGE ExistentialQuantification #-}

--------------------------------------------------------------------------
--
-- Copyright: (c) Javier López Durá
-- License: BSD3
--
-- Maintainer: Javier López Durá <linux.kitten@gmail.com>
--
--------------------------------------------------------------------------

module Data.Ethereum.RLP
    ( Rlp(..)
    , RlpData(..)
    , RlpStream(..)
    , encode
    , decode
    ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word

-- |Estructura de datos a codificar
data RlpData = RlpStr { strBs :: ByteString }
             | RlpList { lstData :: [RlpData] }
             deriving (Show)

-- |Flujo de dato a decodificar
newtype RlpStream = RlpStream { rlpBs :: ByteString }
                  deriving (Show)

-- |Clase para otros tipos puedan codificarse para
-- su codificación/decodificación con 'encode'/'decode'
class Rlp a where
    rlpEncode :: a -> RlpData
    rlpDecode :: RlpData -> a

toBinary :: forall a. (Integral a) => a -> ByteString
toBinary 0 = BS.empty
toBinary x = BS.append (toBinary (x `div` 0xff))
                       (BS.singleton $ fromIntegral (x `mod` 0xff))

fromBinary :: forall a. (Integral a) => ByteString -> a
fromBinary = snd . BS.foldr (\b (p,r) -> (p+1,(fromIntegral b * (0xff ^ p))+r)) (0,0)

encInt :: forall a. Integral a => a -> RlpData
encInt = RlpStr . toBinary

decInt :: forall a. Integral a => RlpData -> a
decInt = fromBinary . strBs

instance Rlp Word16 where
    rlpEncode = encInt
    rlpDecode = decInt

instance Rlp Word32 where
    rlpEncode = encInt
    rlpDecode = decInt

instance Rlp Word64 where
    rlpEncode = encInt
    rlpDecode = decInt

instance Rlp Integer where
    rlpEncode = encInt
    rlpDecode = decInt

instance forall a. (Rlp a) => Rlp [a] where
    rlpEncode = RlpList . map rlpEncode
    rlpDecode = map rlpDecode . lstData

instance Rlp ByteString where
    rlpEncode = RlpStr
    rlpDecode = strBs

-- |Codifica la estructura de datos de entrada
encode :: RlpData -> RlpStream

encode (RlpStr bs) = RlpStream $
    let len = BS.length bs
    in if len == 1 && BS.head bs < 0x80
        then bs
        else BS.append (encodeLen len 0x80) bs

encode (RlpList bss) = RlpStream $
    let bs = BS.concat $ map (rlpBs . encode) bss
    in BS.append (encodeLen (BS.length bs) 0xc0) bs

encodeLen :: Int -> Word8 -> ByteString
encodeLen len offset =
    if len < 56
        then BS.singleton $ fromIntegral len + offset
        else let bl = toBinary $ fromIntegral len
             in BS.append (BS.singleton (fromIntegral (BS.length bl) + offset + 55)) bl

-- |Decodifica el flujo de datos de entrada
decode :: RlpStream -> RlpData
decode = fst . decodeRlp' . rlpBs

decodeRlp' :: ByteString -> (RlpData,ByteString)
decodeRlp' bs =
    let b = BS.head bs
    in if b >= 0xf8
        then let (bss1,bss2) = decodeLen b 0xc0 True bs
             in (RlpList (decodeList bss1),bss2)
       else if b >= 0xc0
        then let (bss1,bss2) = decodeLen b 0xc0 False bs
             in (RlpList (decodeList bss1),bss2)
       else if b >= 0xb8
        then let (bs1,bss2) = decodeLen b 0x80 True bs
             in (RlpStr bs1,bss2)
       else if b >= 0x80
        then let (bs1,bss2) = decodeLen b 0x80 False bs
             in (RlpStr bs1,bss2)
        else (RlpStr $ BS.singleton b, BS.drop 1 bs)

decodeList :: ByteString -> [RlpData]
decodeList bss =
    if BS.null bss
        then []
        else let (rlp,bss') = decodeRlp' bss in rlp : decodeList bss'

decodeLen :: Word8 -> Word8 -> Bool -> ByteString -> (ByteString,ByteString)
decodeLen b offset lenEncoded bs =
    let buf = BS.tail bs
        len = fromIntegral $ b - offset - (if lenEncoded then 55 else 0)
        sub1 = BS.take len buf
        sub2 = BS.drop len buf
    in if lenEncoded
        then let len2 = fromIntegral $ fromBinary sub1
             in (BS.take len2 sub2, BS.drop len2 sub2)
        else (sub1,sub2)

