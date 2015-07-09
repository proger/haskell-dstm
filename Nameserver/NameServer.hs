module Main (main) where

import Control.Distributed.STM.NameService

main :: IO ()
main = nameService gDefaultNameServer