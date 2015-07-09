{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.RetryVar 
                (RetryLog, RetryVar (RetryVar, LinkRetryVar), newRetryVar,
                 suspend, resumeRetryVarAct, resumeFromId, bundledRetryLogs,
                 insertRetryVarAction, deleteRetryVarAction,
                 printGRetryVarActionMap, remPutRetryEnvLine) where

import Control.Concurrent
import qualified Control.Exception as CE (catch)
import qualified Data.Map as M hiding (map)
import Control.Distributed.STM.DebugBase
import Control.Distributed.STM.EnvAddr
import qualified Control.Distributed.STM.Utils as U
import Prelude as P
import System.IO.Unsafe

--------------------
-- Retry Variable --
--------------------

data RetryVar = RetryVar (MVar ())   -- suspend var for retry
                         VarID       -- retry var id
  	      | LinkRetryVar VarLink -- link to remote RetryVar
  deriving Eq

instance Show RetryVar where
  show (RetryVar _ tId) = show (LinkRetryVar (VarLink gMyEnv tId))
  show (LinkRetryVar v) = "(" ++ show v ++ ")"

instance Read RetryVar where
  readsPrec i str = map (\(x,s) -> (LinkRetryVar x,s)) (readsPrec i str)

---------------------
-- RetryVar Access --
---------------------

newRetryVar :: IO RetryVar
newRetryVar = do
  retryMVar <- newEmptyMVar
  newID     <- uniqueId
  return (RetryVar retryMVar newID)

suspend :: RetryVar -> IO ()
suspend (RetryVar retryMVar _) = takeMVar retryMVar
suspend (LinkRetryVar _)       = return () -- internal error

resumeRetryVarAct :: MVar () -> IO ()
resumeRetryVarAct retryMVar = tryPutMVar retryMVar () >> return ()

------------------------
-- Collect STM Action --
------------------------

type RetryLog       = (EnvAddr, RetryLogBundle)
type RetryLogBundle = Either (IO ()) [VarID]

bundledRetryLogs :: RetryVar ->  [RetryLog] -> [RetryLog]
bundledRetryLogs retryVar queue = 
  U.insertWith updRetryLog (retryVarEnv retryVar) singletonRLog queue
    where singletonRLog = singletonRetryLog retryVar

singletonRetryLog :: RetryVar -> RetryLogBundle
singletonRetryLog (RetryVar retryMVar _) = Left (resumeRetryVarAct retryMVar)
singletonRetryLog (LinkRetryVar (VarLink env retryVarId))
  | env == gMyEnv = Left (resumeFromId retryVarId)
  | otherwise     = Right [retryVarId]

updRetryLog :: RetryLogBundle -> RetryLogBundle -> RetryLogBundle
updRetryLog (Left resume) (Left resumes)   = Left (resumes >> resume)
updRetryLog (Right [rVarId]) (Right rVarIds) = Right (rVarId:rVarIds)
updRetryLog _ y = y -- internal error, either local or remote envs, not mixed

retryVarEnv :: RetryVar -> EnvAddr
retryVarEnv (RetryVar _ _)                 = gMyEnv
retryVarEnv (LinkRetryVar (VarLink env _)) = env

--------------------------------
-- RetryVar Remote Access Map --
--------------------------------

type RetryVarAction = IO ()

gRetryVarActionMap :: MVar (M.Map VarID RetryVarAction)
gRetryVarActionMap = unsafePerformIO (newMVar M.empty)

printGRetryVarActionMap :: VarID -> IO ()
printGRetryVarActionMap tVarId = do
  gMap <- readMVar gRetryVarActionMap
  case M.lookup tVarId gMap of
    Nothing -> debugStrLn2 ("tVarID non existing: " ++ show tVarId)
    Just _  -> debugStrLn2 ("tVarID existing " ++ show tVarId)
  debugStrLn2 ("---")


insertRetryVarAction :: RetryVar -> IO ()
insertRetryVarAction (RetryVar retryMVar retryVarId) =
  modifyMVar_ gRetryVarActionMap $
    return . M.insertWith (flip const) retryVarId (resumeRetryVarAct retryMVar)
insertRetryVarAction (LinkRetryVar _) = return () -- internal error


deleteRetryVarAction :: RetryVar -> IO ()
deleteRetryVarAction (RetryVar _ retryVarId) =
  modifyMVar_ gRetryVarActionMap $ return . M.delete retryVarId
deleteRetryVarAction (LinkRetryVar _) = return () -- internal error

resumeFromId :: VarID -> IO ()
resumeFromId retryVarId = do
  gMap <- readMVar gRetryVarActionMap
  case M.lookup retryVarId gMap of
    Just act -> act
    Nothing  -> return () -- act already deleted because trans already resumed

-----------------
-- Post Office --
-----------------
-- like remPutMsg, statistics is only difference
remPutRetryEnvLine :: Show a => EnvAddr -> a -> IO ()
remPutRetryEnvLine env msg = CE.catch (do
  h <- connectToRetryEnv env
  sendTCP msg h
  )(propagateEx "remPutRetryEnvLine")
