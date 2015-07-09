{-# OPTIONS_GHC -XScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-unused-do-bind #-}
{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.TVar (TVar (TVar, LinkTVar), 
             tVarToLink, tVarEnv, isDistErrTVar, 
             ValidLog, CommitLog, 
             lockAct, unLockAct, validAct, extWaitQAct, commitAct, notifyAct,
             CommitRemVal, ValidRemVal, MaybeRead,
             TransID, RemCont (Com, Ret, End, Err),
             STMMessage (RemResume,RemAddEnvToAction,RemDelEnvFromAction
             ,RemLifeCheck,RemReadTVar,RemStartTrans,RemContTrans
             ,RemElectedNewCoord),
             remGetMsg, remPutMsg, bundledValidLogs, bundledCommitLogs,
             addEnvToTVarActions, delEnvFromTVarActions,
             lockTVarFromId, unLockTVarFromId, readTVarFromId, writeTVarFromId,
             validateTVarFromId, notifyFromId, extWaitQFromId,
             gTVarActionMap,remLock,remValidate,
             readTVarValFromId, swapTVarValFromId) where

import Control.Concurrent
import qualified Control.Exception as CE (catch,throw,SomeException)
import Control.Monad
import qualified Data.List
import qualified Data.Map as DM hiding (map) 
import Control.Distributed.STM.DebugBase
import Control.Distributed.STM.Dist
import Control.Distributed.STM.EnvAddr
import Control.Distributed.STM.Utils hiding (insertWith)
import qualified Control.Distributed.STM.Utils as U
import Prelude as P hiding (catch, putStr, putStrLn)
import Control.Distributed.STM.RetryVar
import System.Mem.Weak
import System.IO
import System.IO.Unsafe

----------------------------
-- Transactional Variable --
----------------------------

-- |Shared memory locations that support atomic memory transactions. Between
-- different nodes memory is shared using transparent process communication.
-- (TVars are called @host TVars@ when they reside on the process where they
-- have been created by calling 'newTVar'. They are called @link TVars@ on 
-- other processes)
data TVar a = TVar (MVar (a,VersionID)) -- host process local TVar value
	           VarID                -- VersionID used for validity check
		   (MVar ())            -- TVar lock for exclusive verify/commit
                   (MVar [RetryLog])    -- wait queue of suspended transactions
            | LinkTVar VarLink          -- link TVar

instance Show (TVar a) where
  show (TVar _ tId _ _) = show (LinkTVar (VarLink gMyEnv tId))
  show (LinkTVar v)     = "(" ++ show v ++ ")" -- precedence ( ) level of read

instance Read (TVar a) where
  readsPrec i str = map (\(x,s) -> (LinkTVar x,s)) (readsPrec i str)

-----------------
-- TVar Access --
-----------------

tVarEnv :: TVar a -> EnvAddr
tVarEnv (TVar _ _ _ _) = gMyEnv
tVarEnv (LinkTVar (VarLink env _)) = env

tVarToLink :: TVar a -> VarLink
tVarToLink (TVar _ tId _ _) = VarLink gMyEnv tId
tVarToLink (LinkTVar link)  = link

----------------------------------------------------------
-- Collect STM Actions -- lock, validate and make waitQ --
----------------------------------------------------------

type ValidLog       = (EnvAddr, ValidLogBundle)
type ValidLogBundle = Either ([(VarID, LockActs)], ValidActs)
                             [ValidRemVal]

data LockActs = LockActs {
                  lockAct   :: IO (), 
                  unLockAct :: IO ()
                  } deriving Show
data ValidActs = ValidActs {
                   validAct    :: IO Bool, 
                   extWaitQAct :: IO ()
                   } deriving Show

type ValidRemVal    = (VarID, MaybeReadWrite)
type MaybeReadWrite = (MaybeRead, Maybe String)
type MaybeRead      = Maybe (VersionID, RetryVar)


bundledValidLogs :: TVar a -> MaybeRead -> [ValidLog] -> [ValidLog]
bundledValidLogs tVar versIdRetryVar validLog =
  -- tVarEnv == primary key
  U.insertWith updValidLog tVEnv singletonVLog validLog
    where singletonVLog = singletonValidLog tVar versIdRetryVar
          tVEnv = tVarEnv tVar

singletonValidLog :: TVar a -> MaybeRead -> ValidLogBundle
-- orig TVar: read TVar -> validate action, curr transaction -> xtd waitQ action
singletonValidLog (TVar ref tId lock waitQ) (Just (versionId, retryVar)) = Left
  ([(tId, LockActs {lockAct   = takeMVar lock,
                    unLockAct = putMVar lock () })],
   ValidActs {validAct    = readMVar ref >>= return . (== versionId) . snd,
              extWaitQAct = modifyMVar_ waitQ insertRetry })
     where insertRetry = return . (bundledRetryLogs retryVar)
-- orig TVar: write TVar -> no "start"-actions other than locking
singletonValidLog (TVar _ tId lock _) Nothing = Left
  ([(tId, LockActs {lockAct   = takeMVar lock,
                    unLockAct = putMVar lock () })],
   ValidActs {validAct = return True, extWaitQAct = return ()})
-- rem TVar: read TVar -> validate action, curr transaction -> xtd waitQ action
singletonValidLog (LinkTVar (VarLink env tId)) (Just (versionId, retryVar))
  | env == gMyEnv = Left 
     ([(tId, LockActs{lockAct   = lockTVarFromId tId,
                      unLockAct = unLockTVarFromId tId })],
      ValidActs {validAct    = validateTVarFromId (tId, versionId),
                 extWaitQAct = extWaitQFromId (tId, retryVar)})
    -- rem TVar: read TVar -> validate data, curr transaction -> xtd waitQ data
  | otherwise = Right [(tId, (Just (versionId, retryVar), Nothing))]
-- rem TVar: write TVar -> no "start"-data other than needed for locking
singletonValidLog (LinkTVar (VarLink env tId)) Nothing
  | env == gMyEnv = Left
     ([(tId, LockActs {lockAct   = lockTVarFromId tId,
                       unLockAct = unLockTVarFromId tId })],
      ValidActs {validAct = return True, extWaitQAct = return ()})
  | otherwise = Right [(tId, (Nothing, Nothing))]

updValidLog :: ValidLogBundle -> ValidLogBundle -> ValidLogBundle
updValidLog (Left ([(tId, lock)], val)) (Left (locks, vals)) = Left
  -- tId == secondary key => total order on locks
  (U.insertWith const tId lock locks, 
   ValidActs {validAct    = validAct val >>+ validAct vals,
              extWaitQAct = extWaitQAct val >> extWaitQAct vals })
updValidLog (Right [(tId, rwVal)]) (Right rwVals) = Right
  (U.insertWith mergeRVal tId rwVal rwVals)
updValidLog _ y = y -- internal error

mergeRVal :: MaybeReadWrite -> MaybeReadWrite -> MaybeReadWrite
mergeRVal (Nothing, _) v = v
mergeRVal (Just r, w) _  = (Just r, w)

----------------------------------------------
-- Collect STM Actions -- commit and notify --
----------------------------------------------

type CommitLog       = (EnvAddr, CommitLogBundle)
type CommitLogBundle = Either [(VarID, CommitActs)] ([CommitRemVal], RegIO)

data CommitActs = CommitActs {
                    commitAct   :: IO (), 
                    notifyAct :: IO ()
                  } deriving Show

type CommitRemVal = (VarID, String)
type RegIO        = IO ()

bundledCommitLogs :: Dist a => TVar a -> a -> [CommitLog] -> [CommitLog]
bundledCommitLogs tVar value commitLog =
  U.insertWith updCommit tVEnv singletonCLog commitLog
    where singletonCLog = singletonCommitLog tVar value
          tVEnv = tVarEnv tVar

singletonCommitLog :: Dist a => TVar a -> a -> CommitLogBundle
singletonCommitLog (TVar ref tId _ waitQ) value = Left
  [(tId, CommitActs{commitAct= modifyMVar_ ref incVersId,
                    notifyAct= swapMVar waitQ [] >>= mapM_ coreNotify })]
           where incVersId =
                   return . (,)value . (+1) . snd
singletonCommitLog (LinkTVar (VarLink env tId)) value
  | env == gMyEnv = 
    Left [(tId, CommitActs{commitAct = writeTVarFromId (tId, show value),
                           notifyAct = notifyFromId tId
                         })]
  | otherwise     = Right ([(tId, show value)], regTVars env value)

updCommit :: CommitLogBundle -> CommitLogBundle -> CommitLogBundle
updCommit (Left [(tId, comNfyAct)]) (Left comNfys) = 
  Left (U.insertWith const tId comNfyAct comNfys)
updCommit (Right ([(tId, val)], reg)) (Right (idStrs, regs)) = 
  Right (U.insertWith const tId val idStrs, regs >> reg)
updCommit _ y = y -- internal error

----------------------------
-- TVar Remote Access Map --
----------------------------

data TVarActions = TVarActions {
                   remRead     :: EnvAddr -> IO String,
	           remWrite    :: String -> IO (),
                   remReadVal  :: IO String,
	           remSwapVal  :: String -> IO String,
	           remValidate :: VersionID -> IO String,
                   remLock     :: IO (),
                   remUnLock   :: IO (),
                   remExtWaitQ :: RetryVar -> IO (),
                   remNotify   :: IO ()
                   }

gTVarActionMap :: MVar (DM.Map VarID (TVarActions, [EnvAddr]))
{-# NOINLINE gTVarActionMap #-}
gTVarActionMap = unsafePerformIO (newMVar DM.empty)

insertTVarActions :: Dist a => TVar a -> EnvAddr -> IO ()
insertTVarActions (TVar tVarRef tVarId tVarLock waitQ) fstDestEnv =
  let tVarActions = TVarActions {
        -- regular read of host TVar value and register possibly exported TVars
        remRead     = \destEnv -> do 
                     -- read lock to prevent read before finishing remote commit
                                  takeMVar tVarLock
                                  v'@(v, _) <- readMVar tVarRef
                                  putMVar tVarLock () -- read lock
                                  regTVars destEnv v
                                  debugStrLn8 ("###>>> remRead "++show v')
                                  return (show v'),
        -- regular write to host TVar, incr. version and finalize received TVars
        remWrite    = \str -> do
                              debugStrLn8 ("###>>> remWrite "++show str)
                              let v = read str
                              modifyMVar_ tVarRef (return .((,) v) .((+1) .snd))
                              finTVars v,
        -- "non-destructive" read of uncommitted writes -> no TVar register
        remReadVal  = readMVar tVarRef >>= return . show,
        -- "nd" swap of uncommitted writes -> no version increment and finalize
        remSwapVal =  liftM show . swapMVar tVarRef . read,
        remValidate = \versionId -> do 
                                    (_, vId) <- readMVar tVarRef
                                    return (show (versionId == vId)),
        remLock     = takeMVar tVarLock,
        remUnLock   = putMVar tVarLock (),
        remExtWaitQ = \retryVar -> modifyMVar_ waitQ 
                                        (return . (bundledRetryLogs retryVar)),
        remNotify   = do queue <- swapMVar waitQ []
                         mapM_ coreNotify queue
        } in
  modifyMVar_ gTVarActionMap $
   return . DM.insertWith (\_ (acts, envs) -> (acts, (fstDestEnv:envs))) 
              tVarId (tVarActions, [fstDestEnv])
insertTVarActions (LinkTVar _) _ = return () -- internal error

coreNotify :: RetryLog -> IO ()
coreNotify (_, Left resumeAct) = resumeAct
coreNotify (env, Right retryVarIds) = do
  forkIO $ CE.catch (remPutRetryEnvLine env (RemResume retryVarIds)
    )(\(e::CE.SomeException) -> debugStrLn1 ("coreNotify "++show e))
  return ()-- ignore notification of lost nodes

addEnvToTVarActions :: VarID -> EnvAddr -> IO ()
addEnvToTVarActions tVarId env = do
  modifyMVar_ gTVarActionMap 
    (return . (DM.insertWith (\_ (as, envs) -> (as, (env:envs))) 
               tVarId (error ("error addEnvToTVarActions: TVarId "++
                               show tVarId ++" not listed"))))

delEnvFromTVarActions :: VarID -> EnvAddr -> IO ()
delEnvFromTVarActions tVarId env = do
  oldMap <- takeMVar gTVarActionMap
  case DM.lookup tVarId oldMap of
    Nothing -> error ("error trying to remove non-existing TVar: "++show tVarId)
    Just (acts, envs) ->
      putMVar gTVarActionMap 
              (case Data.List.delete env envs of
                 [] -> DM.delete tVarId oldMap -- no more refs to tVar
                 newEnvs -> DM.insert tVarId (acts, newEnvs) oldMap)

readTVarFromId :: VarID -> EnvAddr -> (String -> IO ()) -> IO ()
readTVarFromId tVarId destEnv sendReply = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remRead (fst (aMap DM.! tVarId)) destEnv >>= sendReply
  )(distInstanceError "readTVarFromId")

writeTVarFromId :: (VarID, String) -> IO ()
writeTVarFromId (tVarId, str) = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remWrite (fst (aMap DM.! tVarId)) str
  )(distInstanceError "writeTVarFromId")

readTVarValFromId :: VarID -> IO String
readTVarValFromId tVarId = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remReadVal (fst (aMap DM.! tVarId))
  )(\e -> (distInstanceError "readTVarValFromId" e) >> return "")

swapTVarValFromId :: VarID -> String -> IO String
swapTVarValFromId tVarId v' = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remSwapVal (fst (aMap DM.! tVarId)) v'
  )(\e -> (distInstanceError "swapTVarValFromId" e) >> return "")

lockTVarFromId :: VarID -> IO ()
lockTVarFromId tVarId = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remLock (fst (aMap DM.! tVarId))
  return ()
  )(distInstanceError "lockTVarFromId")

unLockTVarFromId :: VarID -> IO ()
unLockTVarFromId tVarId = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remUnLock (fst (aMap DM.! tVarId))
  )(distInstanceError "unLockTVarFromId")

validateTVarFromId :: (VarID, VersionID) -> IO Bool
validateTVarFromId (tVarId, versionId) = CE.catch (do
  aMap <- readMVar gTVarActionMap
  answer <- remValidate (fst (aMap DM.! tVarId)) versionId
  return (read answer)
  )(\e -> (distInstanceError "validateTVarFromId" e) >> return False)

extWaitQFromId :: (VarID, RetryVar) -> IO ()
extWaitQFromId (tVarId, retryVar) = CE.catch (do
  aMap <- readMVar gTVarActionMap
  remExtWaitQ (fst (aMap DM.! tVarId)) retryVar
  )(distInstanceError "extWaitQFromId")

notifyFromId :: VarID -> IO ()
notifyFromId tVarId = CE.catch (do
  aMap <- readMVar gTVarActionMap
  forkIO (CE.catch (remNotify (fst (aMap DM.! tVarId))) 
         (\(e::CE.SomeException) -> debugStrLn1 ("notifyFromId " ++ show e)))
  return ()
  )(distInstanceError "notifyFromId")

distInstanceError :: String -> CE.SomeException -> IO ()
distInstanceError s e = do
  putStrLn ("\nDSTM Program Error in " ++ s ++ 
            "\nPossibly missing Dist instance for TVar data type")
  CE.throw e
  

-----------------
-- Post Office --
-----------------

remPutMsg :: Show a => EnvAddr -> a -> IO ()
remPutMsg env msg = CE.catch (do
  h <- connectToEnvHold env
  sendTCP msg h
  )(propagateEx "remPutMsg")

remGetMsg :: Show a => EnvAddr -> a -> IO String
remGetMsg env msg = CE.catch (
  connectAtomicToEnv env (recvTCP msg)
  )(propagateEx "remGetMsg")

----------------------
-- Remote Messaging --
----------------------

type TransID  = (EnvAddr, STMID) 

data RemCont  = Com | Ret | End | Err 
  deriving (Show, Read, Eq)

data STMMessage = RemResume           [VarID] -- RetryVarIDs
		| RemAddEnvToAction   VarID EnvAddr
		| RemDelEnvFromAction VarID EnvAddr
		| RemLifeCheck
                | RemReadTVar         VarID EnvAddr
	        | RemStartTrans       TransID [ValidRemVal] [EnvAddr]
		| RemContTrans        TransID RemCont
		| RemElectedNewCoord  TransID RemCont [EnvAddr]
  deriving (Show, Read)

-------------------------------
-- Class Dist TVar instances --
-------------------------------

instance Dist a => Dist (TVar a)  where
  finTVars (TVar _ _ _ _) = return () -- internal error

  finTVars remTVar@(LinkTVar linkTVar) = CE.catch (do
    addFinalizer remTVar (finalizeTVar linkTVar)
    )(\(e::CE.SomeException) -> debugStrLn1 ("### Error finTVars: " ++ show linkTVar ++ show e)) --return ()) -- ignore errors in dead process TVars

-- remember locally which (1.) node(s) are using my host TVar
  regTVars destEnv tVar@(TVar _ _ _ _) = insertTVarActions tVar destEnv

-- make linkTVar node remember locally that dest node is using it
  regTVars destEnv (LinkTVar (VarLink tEnv tVarId)) = CE.catch (
    remPutMsg tEnv (RemAddEnvToAction tVarId destEnv)
    )(\(e::CE.SomeException) -> debugStrLn1 ("### Error regTVars: " ++ show tEnv ++ " -> destEnv: " ++ show destEnv ++ show e) )--return ()) -- ignore errors in regTVar (to not prohibit remRead)

-- make linkTVar node forget locally that my node was using it (enable GC)
finalizeTVar :: VarLink -> IO ()
finalizeTVar (VarLink tEnv tVarId) = do
  remPutMsg tEnv (RemDelEnvFromAction tVarId gMyEnv)

---------------------------
-- TVar Robustness Check --
---------------------------

-- |'isDistErrTVar' @e@ @tVar@ checks whether @tVar@ is unreachable when 
-- exception @e@ had been raised. It returns 'True' if the exception raised 
-- denotes @tVar@ as unreachable, 'False' otherwise. A TVar returning 'True' 
-- once will never return a 'False' check result.
isDistErrTVar :: SomeDistTVarException -> TVar a -> Bool
isDistErrTVar e tVar = distTVarExEnv e == tVarEnv tVar

