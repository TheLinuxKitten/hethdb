
--------------------------------------------------------------------------
--
-- Copyright: (c) Javier López Durá
-- License: BSD3
--
-- Maintainer: Javier López Durá <linux.kitten@gmail.com>
--
--------------------------------------------------------------------------

module Data.Ethereum.EthProto
  ( EthProto(..)
  , EthProtoCfg(..)
  , publicEthProtoCfg
  , ethProto
  ) where

import Network.Web3.Types

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


