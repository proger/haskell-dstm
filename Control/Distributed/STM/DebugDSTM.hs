{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.DebugDSTM
                  (module Control.Distributed.STM.DebugBase, 
                   printValid, printValidFromId, proc, printStatistics) where

import Control.Distributed.STM.DebugBase
import Control.Distributed.STM.EnvAddr
import Control.Distributed.STM.STM
import Control.Distributed.STM.TVar
import Prelude as P hiding (catch, putStr, putStrLn)

------------------------
-- Debugging Routines --
------------------------

printValid :: String -> TransID -> ValidLog -> IO ()
printValid str transId (env, startVals) = do
  debugStrLn8 (">>>   printValid "++str++"   <<<")
  debugStrLn8 ("TransID = "++show transId)
  case startVals of
    Left idActsRVal -> 
      debugStrLn8 ("loc env = "++show env++show ((fst.unzip.fst) idActsRVal))
    Right startRemVals -> do
      debugStrLn8 ("rem env = "++show env)
      mapM_ printRemVal startRemVals
  debugStrLn8 ("<<<   printValid")

printRemVal :: ValidRemVal -> IO ()
printRemVal (vId, (rVal, wVal)) = do
  debugStrLn8 ("     id = "++show vId++" rVal = "++show rVal++" wVal = "++show wVal)

printValidFromId :: String -> TransID -> [ValidRemVal] -> IO ()
printValidFromId str transId startRemVals = do
  debugStrLn8 (">>>   printValidFromId "++str++"   <<<")
  debugStrLn8 ("TransID = "++show transId)
  mapM_ printRemVal startRemVals
  debugStrLn8 ("<<<   printValidFromId")

proc :: IO a -> STM a
proc io = STM (\stmState -> io >>=  return . Success stmState)

printStatistics :: String -> IO ()
printStatistics = printTCPStat