{-# OPTIONS_GHC -XScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-unused-do-bind #-}

module Control.Distributed.STM.DSTM (TVar, STM, newTVar, readTVar, writeTVar,
             atomic, retry, orElse, throw, catch, 
             module Control.Distributed.STM.Dist,
             module Control.Distributed.STM.NameService,
             startDist, SomeDistTVarException, isDistErrTVar) where

import Control.Concurrent
import Control.Distributed.STM.DebugDSTM
import Control.Distributed.STM.Dist
import Control.Distributed.STM.EnvAddr
import Control.Distributed.STM.NameService -- just for export
import Control.Distributed.STM.RetryVar
import Control.Distributed.STM.STM
import Control.Distributed.STM.TVar
import Control.Distributed.STM.Utils
import Control.Exception hiding (catch, throw)
import qualified Control.Exception as CE (catch, throw)
import Control.Monad
import qualified Data.List
import Network
import Network.Socket hiding (accept)
import Prelude as P hiding (catch)
import System.IO
import System.IO.Unsafe
import System.Mem
import System.Process (ProcessHandle, runCommand, terminateProcess)
import System.Posix

infixl 2 `orElse`

------------------
-- TVar actions --
------------------

gInitVersionID :: ID
gInitVersionID = 1
 
-- | Create a new TVar holding a value supplied
newTVar   :: Dist a => a -> STM (TVar a)
newTVar v = STM (\stmState -> do
  newID   <- uniqueId
  newRef  <- newMVar (v, gInitVersionID)
  newLock <- newMVar ()
  waitQ   <- newMVar []
-- newDebugMVar enables later MVar inspection
--  newRef  <- newDebugMVar ("Ref:   TVar "++show newID) (v, gInitVersionID)
--  newLock <- newDebugMVar ("Lock:  TVar "++show newID) ()
--  waitQ   <- newDebugMVar ("WaitQ: TVar "++show newID) []
  let tVar = (TVar newRef newID newLock waitQ)
  return (Success stmState tVar))
 
-- |Return the current value stored in a TVar
readTVar  :: Dist a => TVar a -> STM a
readTVar tVar = STM (\stmState -> do
  (val, vId) <- readIntraTransTVar tVar (stmCommit stmState)
  let newState = stmState
        {stmValid = bundledValidLogs tVar
                                    (Just (vId, (stmRetryVar stmState)))
                                    (stmValid stmState)
        }
  return (Success newState val))

-- check if uncommitted transaction-local writes precede this read
readIntraTransTVar :: Dist a => TVar a -> [CommitLog] -> IO (a, VersionID)
readIntraTransTVar  tVar commitLog =
  case P.lookup env commitLog of
    Just (Left commitActs) ->
      case P.lookup tId commitActs of
        Just acts -> readHost tVar $ commitAct acts
        Nothing   -> coreReadTVar tVar
    Just (Right (strVals, _)) ->
      case P.lookup tId strVals of
        Just v  -> coreReadTVar tVar >>= return . ((,) (read v)) . snd
        Nothing -> coreReadTVar tVar
    Nothing -> coreReadTVar tVar
  where VarLink env tId = tVarToLink tVar

coreReadTVar  :: Dist a => TVar a -> IO (a,VersionID)
coreReadTVar (TVar tVarRef _ lock _) = do
  takeMVar lock -- read lock to prevent read before finishing remote commit
  v <- readMVar tVarRef 
  putMVar lock ()
  debugStrLn8 ("coreReadTVar Loc "++show v)
  return v
coreReadTVar (LinkTVar (VarLink tEnv tVarId)) = CE.catch (do   
  answer <- remGetMsg tEnv (RemReadTVar tVarId gMyEnv)
  let vv@(v,_) = read answer
  finTVars v
  debugStrLn8 ("coreReadTVar Rem "++show vv)
  return vv
  )(propagateEx "coreReadTVar")

-- read uncommitted transaction-local writes by keeping original version id
readHost :: forall a . Read a => TVar a -> IO () -> IO (a, VersionID)
            -- explicit forall a extends scope of a into function body
readHost (TVar tVarRef _ lock _) commit = do
  takeMVar lock
  orig@(_, origVersion) <- readMVar tVarRef
  commit
  (modV, _) <- swapMVar tVarRef orig
  putMVar lock ()
  return (modV, origVersion)
readHost (LinkTVar (VarLink _ tId)) commit = do
  lockTVarFromId tId
  origStr <- readTVarValFromId tId
  let (_::a, origVersion) = read origStr
  commit
  (modV, _::VersionID) <- liftM read (swapTVarValFromId tId origStr)
  unLockTVarFromId tId
  return (modV, origVersion)

-- |Write the supplied value into a TVar
writeTVar :: Dist a => TVar a -> a -> STM ()
writeTVar tVar v = STM (\stmState -> 
  let newState = stmState{
    stmValid  = bundledValidLogs tVar Nothing (stmValid stmState),
    stmCommit = bundledCommitLogs tVar v (stmCommit stmState)
    }
  in debugStrLn8 ("writeTVar "++show (stmCommit newState))>>return (Success newState ()))

----------------------
-- STM computations --
----------------------

-- |Perform a series of STM actions atomically
atomic :: Show a => STM a -> IO a
atomic stmAction = do
  aStat ATM
  debugStrLn7 ("A")
  iState <- initialState
  debugStrLn7 ("I")
  stmResult <- runSTM stmAction iState
  debugStrLn7 ("M")
  case stmResult of
    Exception newState e -> do 
      debugStrLn7 ("E")
      let gState = gatherStmState newState
      valid <- startTrans gState
      if valid
        then do
             endTrans gState
             CE.throw e -- propagate Exception into IO world
        else do
             endTrans gState
             atomic stmAction
    Retry newState -> do
      debugStrLn7 ("R")
      let gState = gatherStmState newState
      valid <- startTrans gState
      if valid
        then do
             debugStrLn7 ("v")
             retryTrans gState  -- including set auto-link
             endTrans gState
             let retryVar = stmRetryVar gState
             insertRetryVarAction retryVar
             debugStrLn7 ("SUSPEND "++show (stmId gState))
             suspend retryVar
             debugStrLn7 ("RESUME "++show (stmId gState))
             deleteRetryVarAction retryVar
	     unRetryTrans gState  -- reset auto-link
        else do
    	     debugStrLn7 ("i")
             endTrans gState
             aStat INV
      atomic stmAction
    Success newState res -> do
      debugStrLn7 ("S")
      let gState = gatherStmState newState
      valid <- startTrans gState
      if valid
        then do
             debugStrLn7 ("v")
             commitTrans gState
             endTrans gState
             return res
        else do
   	     debugStrLn7 ("i")
             endTrans gState
             aStat INV
             atomic stmAction

initialState :: IO STMState
initialState = do
  atomicId <- uniqueId
  retryVar <- newRetryVar
  return (STMState {stmId       = (gMyEnv, atomicId),
                    stmRetryVar = retryVar,
	            stmValid    = [],
                    stmCommit   = []})

-- |Retry execution of the current memory transaction because it has seen 
-- values in TVars which mean that it should not continue (e.g. the TVars
-- represent a shared buffer that is now empty). The implementation may block
-- the thread until one of the TVars that it has read from has been udpated.
retry  :: STM a
retry = STM (\state -> return (Retry state))

-- |Compose two alternative STM actions. If the first action completes without
-- retrying then it forms the result of the orElse. Otherwise, if the first
-- action retries, then the second action is tried in its place. If both
-- actions retry then the orElse as a whole retries
orElse :: STM a -> STM a -> STM a
orElse (STM stm1) (STM stm2) =
  STM (\(stmState@STMState{stmCommit = savedCont}) -> do
         stm1Res <- stm1 stmState
         case stm1Res of
           Retry newState -> stm2 newState{stmCommit = savedCont}
	   _              -> return stm1Res)

----------------
-- Exceptions --
----------------

-- |Throw an exception within an STM action
throw :: SomeException -> STM a
throw e = STM (\state -> return (Exception state e))

-- |Exception handling within STM actions
catch :: STM a -> (SomeException -> STM a) -> STM a
catch (STM stm) eHandler = STM (\stmState -> do
  res <- stm stmState
  case res of
    Exception _ e -> do
                     let (STM stmEx) = eHandler e
                     stmEx stmState
    _             -> return res)


-------------------------
-- Perform STM Actions --
-------------------------

-- copy WriteVals from CommitVals to ValidVals at end of transaction because of
-- possible nested orElse-scopes where locks and validations are preserved
-- while commits are reset using simple push/pop in orElse stack 
-- finally all info is needed in ValidVals to be transmitted to all trans
-- to ensure proper recovery in 3-phase model

gatherStmState :: STMState -> STMState
gatherStmState state = state {stmValid =
  map (gatherValidRemVal (stmCommit state)) (stmValid state)}

gatherValidRemVal :: [CommitLog] -> ValidLog -> ValidLog
gatherValidRemVal _ (env, Left stActs) = (env, Left stActs)
gatherValidRemVal comLogs vLog@(env, Right validRemVals) =
  case P.lookup env comLogs of
    Nothing             -> vLog
    Just (Right (v, _)) -> (env, Right(map (addCommitRemVals v) validRemVals))
    _                   -> vLog -- internal error

addCommitRemVals :: [CommitRemVal] -> ValidRemVal -> ValidRemVal
addCommitRemVals comVals (tId,(rVal, _)) = (tId, (rVal, P.lookup tId comVals))

-------------------
startTrans :: STMState -> IO Bool
startTrans state = do
  robustFoldValidAct (stmId state) (fst.unzip $ valLogs) True valLogs
  where valLogs = stmValid state

robustFoldValidAct :: TransID -> [EnvAddr] -> Bool -> [ValidLog] -> IO Bool
robustFoldValidAct _ _ isValid [] = return isValid
robustFoldValidAct transId envs isValid (vLog:vLogs) = CE.catch (do
  debugStrLn5 ("robustFoldValidAct: x= "++show vLog++"xs= "++show vLogs)
  b <- doValidAction isValid vLog transId envs
  robustFoldValidAct transId envs b vLogs
  )(\(e::SomeDistTVarException) -> do
    debugStrLn1 (">>> robustFoldValidAct -> error: " ++ show e)
    debugStrLn1 (">>> robustFoldValidAct dyn: "++ show (distTVarExEnv e))
    CE.catch (doEndAction transId vLog) 
             (\(_::SomeDistTVarException) -> return ())
    propagateEx "robustFoldValidAct _" e
   )

doValidAction :: Bool -> ValidLog -> TransID -> [EnvAddr] -> IO Bool
doValidAction isValid (_, Left (lockActs, readAct)) _ _ = do
  mapM_ (lockAct.snd) lockActs
  isValid +>> validAct readAct
doValidAction isValid (env, (Right idVRs)) transId transEnvs =
  remGetMsg env (RemStartTrans transId idVRs transEnvs)
  >>= return . (isValid &&) . read
  -- possible opt: lazy val. msg -> requires skipping recovery on lazy val.

commitTrans :: STMState -> IO ()
commitTrans state = robustMapM_ (doCommitAction (stmId state)) (stmCommit state)

doCommitAction :: TransID -> CommitLog -> IO ()
doCommitAction _ (env, Left idCommits) = do
  debugStrLn5 ("doCommitAction: env= "++show env++"ids= "++show idCommits)
  mapM_ (commitAct.snd) idCommits
  mapM_ (notifyAct.snd) idCommits
doCommitAction transId (env, Right (_, regAct)) = do
  regAct -- register dest env on TVars within values
  remPutMsg env (RemContTrans transId Com)

retryTrans :: STMState -> IO ()
retryTrans state = robustMapM_ (doRetryAction (stmId state)) (stmValid state)

doRetryAction :: TransID -> ValidLog -> IO ()
doRetryAction _ (env, Left (_, valLog)) = do
  debugStrLn5 ("doRetryAction: env= "++show env)
  extWaitQAct valLog
doRetryAction transId (env, Right idrs) = do
  remPutMsg env (RemContTrans transId Ret)
  -- insert auto link or update existing (retry or auto) link with retryVar
  insertRetryLinks env idrs

unRetryTrans :: STMState -> IO ()
unRetryTrans state = mapM_ doUnRetryAction (stmValid state)

doUnRetryAction :: ValidLog -> IO ()
doUnRetryAction (_, Left _)       = return ()
doUnRetryAction (env, Right idrs) = do
  deleteRetryLinks env idrs
  debugStrLn5 ("makeUnWaitQAction: env= "++show env++"idrs= "++show idrs)

endTrans :: STMState -> IO ()
endTrans state = robustMapM_ (doEndAction (stmId state)) (stmValid state)

doEndAction :: TransID -> ValidLog -> IO ()
doEndAction _ (_, Left stLog) = do
  mapM_ (unLockAct.snd) (fst stLog)
doEndAction transId (env, Right _) = do
  remPutMsg env (RemContTrans transId End)

robustMapM_ :: Show a => (a -> IO ()) -> [a] -> IO ()
robustMapM_ _ []          = return ()
robustMapM_ io (x:xs) = CE.catch (do
  debugStrLn5 ("robustMapM_: x= "++show x++"xs= "++show xs)
  io x
  robustMapM_ io xs
  )(\(ex::SomeDistTVarException) -> do
    debugStrLn1 (">>> robustMapM_ -> error: " ++ show ex)
    debugStrLn1 (">>> robustMapM_ dyn: "++ show (distTVarExEnv ex))
    robustMapM_ io xs -- ignore action x and continue w/actions xs, w/o prop.!
   )

---------------------------------
-- Remote Transactional Status --
---------------------------------

type DistTransCont = (TransID, Chan RemCont)

-- state of received remote transactions needed for possible recovery
gDistTransCont :: MVar [DistTransCont]
{-# NOINLINE gDistTransCont #-}
gDistTransCont = unsafePerformIO (newMVar [])

startRemTrans :: TransID -> [ValidRemVal] -> 
                 [EnvAddr] -> (Bool -> IO ()) -> IO ()
startRemTrans transId idVRVars transEnvs notifyCaller = do
  updateAutoTrans (+1) transId
  msgChan <- newChan
  modifyMVar_ gDistTransCont (return . ((transId, msgChan):))
  forkIO (CE.catch
           (ctrlTrans msgChan transId idVRVars transEnvs notifyCaller)
           (\(e::SomeException) -> debugStrLn1 ("startRemTrans " ++ show e)))
  return ()

contRemTrans :: TransID -> RemCont -> IO ()
contRemTrans transId msg = do
  debugStrLn8 ("<<<   contRemTrans transId "++show transId)
  conts <- readMVar gDistTransCont
  case P.lookup transId conts of
    Just msgChan -> do
      debugStrLn8 ("<<<   contRemTrans : transId =  "++show transId)
      writeChan msgChan msg
      debugStrLn8 ("<<<   contRemTrans : writeChan msg =  "++show msg)
    Nothing  -> -- return () -- no error, possible duplicate recovery msgs
      debugStrLn8 ("<<<   contRemTrans N RemCont=   "++show msg)

ctrlTrans :: Chan RemCont -> TransID -> [ValidRemVal] -> 
             [EnvAddr] -> (Bool -> IO ()) -> IO ()
ctrlTrans msgChan transId idVRVars transEnvs notifyCaller = do
  -- start remote transaction (phase 1)
  printValidFromId "ctrlTrans" transId idVRVars
  debugStrLn8 ("<<<   ctrlTrans lock transId "++show transId ++ " idVRVars "++ show idVRVars)
  mapM_ (lockTVarFromId.fst) idVRVars
  debugStrLn8 ("###   ctrlTrans locked transId=   "++show transId)
  isValid <- foldr validateValidIds (return True) idVRVars
  CE.catch (notifyCaller isValid) 
           (\(e::SomeException) -> do
             debugStrLn1 $ "<<<   ctrlTransFromIds !!! catch=   " ++ show e
             writeChan msgChan Err
           )
  ctrlContTrans msgChan transId idVRVars transEnvs

ctrlContTrans :: Chan RemCont -> TransID -> [ValidRemVal] -> 
                 [EnvAddr] -> IO ()
ctrlContTrans msgChan transId idVRVars transEnvs = do
  -- continue remote transaction (phase 2)
  debugStrLn8 (">   ctrlContTrans readChan ")
  msg <- readChan msgChan
  debugStrLn8 ("<   ctrlContTrans readChan ")
  case msg of
    Com -> do -- decision for Com
      debugStrLn8 ("<<<   ctrlContTrans: Com transId =   "++show transId)
      mapM_ contWriteTVar idVRVars
      mapM_ (notifyFromId.fst) idVRVars
      ctrlEndTrans msgChan transId Com idVRVars transEnvs 
    Ret -> do -- decision for Ret
      debugStrLn8 ("<<<   ctrlContTrans: Ret transId =   "++show transId)
      mapM_ contExtWaitQ idVRVars
      ctrlEndTrans msgChan transId Ret idVRVars transEnvs
    End -> do -- decision for rerun (invalid) trans
      debugStrLn8 ("<<<   ctrlContTrans: End/Invalid transId =   "++show transId)
      finishTrans transId idVRVars
    Err -> do -- error before decision
      debugStrLn8 ("<<<   ctrlContTrans: Err transId =   "++show transId)
      electNewTransCoordinator transId transEnvs End
      ctrlContTrans msgChan transId idVRVars transEnvs

ctrlEndTrans :: Chan RemCont -> TransID -> RemCont -> 
                [ValidRemVal] -> [EnvAddr] -> IO ()
ctrlEndTrans msgChan transId remCont idVRVars transEnvs = do
  -- finish remote transaction (phase 3)
  debugStrLn8 (">   ctrlEndTrans readChan ")
  msg <- readChan msgChan
  debugStrLn8 ("<   ctrlEndTrans readChan ")
  case msg of
    End -> do -- finish trans ok
      debugStrLn8 ("<<<   ctrlEndTrans: End transId =   "++show transId ++ " idVRVars "++ show idVRVars)
      finishTrans transId idVRVars
      debugStrLn8 ("<<<   ctrlEndTrans: End unlocked transId =  "++show transId)
    Err -> do -- error after decision
      debugStrLn8 ("<<<   ctrlEndTrans: Err transId =   "++show transId)
      debugStrLn8 ("### ctrlEndTrans phase III cont trans w/new initiator")
      electNewTransCoordinator transId transEnvs remCont
      ctrlEndTrans msgChan transId remCont idVRVars transEnvs
      debugStrLn8 ("###   ctrlEndTrans  ok")
    m -> if m == remCont -- no error, possible duplicate recovery msgs
           then ctrlEndTrans msgChan transId m idVRVars transEnvs 
           else error "error ctrlEndTrans"

finishTrans :: TransID -> [ValidRemVal] -> IO ()
finishTrans trId idVRs = do
  mapM_ (unLockTVarFromId.fst) idVRs
  updateAutoTrans (+ (-1)) trId
  modifyMVar_ gDistTransCont (return . P.filter ((/= trId).fst))

electNewTransCoordinator :: TransID -> [EnvAddr] -> RemCont -> IO ()
electNewTransCoordinator transId@(env, _) transEnvs remCont = do
  let upEnvs = P.filter (/= env) transEnvs
  case minimum upEnvs of
    newC | newC == gMyEnv -> contTransWNewCoord transId remCont upEnvs
         | otherwise -> remPutMsg newC 
                                  (RemElectedNewCoord transId remCont upEnvs)
  `CE.catch` -- newCoordinator unavailable itself (SomeDistTVarException)
  \ex -> let eEnv = (/= distTVarExEnv ex)
             upEnvs = P.filter eEnv . P.filter (/= env) $ transEnvs
  in electNewTransCoordinator transId upEnvs remCont
  `CE.catch` -- other exception
  \(e1::SomeException) -> debugStrLn1 ("electNewTransCoordinator other ex"++show e1)

contTransWNewCoord :: TransID -> RemCont -> [EnvAddr] -> IO ()
contTransWNewCoord transId remCont upEnvs = do -- recovery
  debugStrLn1 (">>><<< contTransWNewCoord 1 : "++show remCont)
  let remEnvs = P.filter (/= gMyEnv) upEnvs
  case remCont of
    End -> return ()
    _   -> do -- continue with trans coordination
         debugStrLn1 (">>><<< contTransWNewCoord 2 : "++show remCont)
         robustMapM_ (flip remPutMsg  (RemContTrans transId remCont)) remEnvs
  robustMapM_ (flip remPutMsg (RemContTrans transId End)) remEnvs
  contRemTrans transId End -- end local

validateValidIds :: ValidRemVal -> IO Bool -> IO Bool
validateValidIds (_, (Nothing, _)) isValid               = isValid
validateValidIds (tId, (Just (versionId, _), _)) isValid =
  validateTVarFromId (tId, versionId) >>+ isValid

contWriteTVar :: ValidRemVal -> IO ()
contWriteTVar (_, (_, Nothing)) = return ()
contWriteTVar (tId, (_, Just str)) = writeTVarFromId (tId, str)

contExtWaitQ :: ValidRemVal -> IO ()
contExtWaitQ (_, (Nothing, _)) = return ()
contExtWaitQ (tId, (Just (_, rVar), _)) = extWaitQFromId (tId, rVar)

-----------------------
-- Distributed Stuff --
-----------------------

nodeReceiver :: Socket -> IO () -- new socket for new sender process
nodeReceiver sock = CE.catch (do
  setSocketOption sock NoDelay 1
  time <- getProcessTimes
  debugStrLn1 ("accept: "++show (elapsedTime time))
  (h, hn, p) <- accept sock -- wait for new process msg handle on socket
  debugStrLn4 ("### new connection: " ++ show h ++ " -> name: " ++ show hn ++ " -> port: " ++ show p)
  hSetBuffering h LineBuffering
  forkIO (readMsg h)
  nodeReceiver sock
  )(\(e::SomeException) -> debugStrLn1 ("nodeReceiver "++show e))

readMsg :: Handle -> IO ()
readMsg h = CE.catch (do
  str <- hGetLine h -- wait for new message on handle
  handleMsg h (read str)
  readMsg h
  )(\(e::SomeException) -> debugStrLn1 ("readMsg dropLostSocket "++show e))

handleMsg :: Handle -> STMMessage -> IO ()
handleMsg h msg =
  case msg of
    RemResume retryVarIds ->
      mapM_ resumeFromId retryVarIds >> aStat RES
    RemAddEnvToAction tVarId env ->
      addEnvToTVarActions tVarId env >> aStat AEA
    RemDelEnvFromAction tVarId env -> 
      delEnvFromTVarActions tVarId env >> aStat DEA
    RemLifeCheck -> 
      debugStrLn2 ("RemLifeCheck: I'm alive") >> aStat LFC
    RemReadTVar tId destEnv -> -- async
      readTVarFromId tId destEnv (hPutStrLn h) >> aStat RDT
    RemStartTrans transId idVRVars transEnvs -> -- async
      startRemTrans transId idVRVars transEnvs (hPutStrLn h . show)
      >> aStat STT
    RemContTrans transId remCont -> 
      contRemTrans transId remCont >> aStat CTT
    RemElectedNewCoord transId remCont oprtlEnvs -> 
      contTransWNewCoord transId remCont oprtlEnvs >> aStat ENC

--------------------
-- TVar Lifecheck --
--------------------

data AutoLink = AutoLink {autoTrans :: Int,  -- stack of open remote trans
                          autoRetry :: [RetryVar]} -- active retry vars
  deriving Show

gDefaultLink :: AutoLink
gDefaultLink =  AutoLink {autoTrans = 0, autoRetry = []}

type Link  = (EnvAddr, AutoLink)

gLinks :: MVar [Link]
{-# NOINLINE gLinks #-}
gLinks = unsafePerformIO (newMVar [])

gTransChkIntv, gRetryChkIntv :: Int
gTransChkIntv    = 1
gRetryChkIntv    = 3

gMegaMuSec :: Int
gMegaMuSec = 1000000


updateAutoTrans ::  (Int -> Int) -> TransID -> IO ()
updateAutoTrans f (env, _) = do 
  links <- takeMVar gLinks
  case Data.List.partition ((== env).fst) links of
    ([], otherLinks) -> do -- no link to env yet
      putMVar gLinks ((env, gDefaultLink{autoTrans = f (autoTrans gDefaultLink)}):otherLinks)
      forkIO (lifeCheck env) -- start new life check loop
      return ()
    ([(e, link)], otherLinks) -> do -- existing link, modify link only
      putMVar gLinks ((e, link{autoTrans = f (autoTrans link)}):otherLinks)
    _ -> putMVar gLinks links -- internal error

insertRetryLinks ::  EnvAddr -> [ValidRemVal] -> IO ()
insertRetryLinks env idVRs = do -- env mapped over other processes in trans
  --printGLinkMap "insertRetryLinks"
  links <- takeMVar gLinks
  case Data.List.partition ((== env).fst) links of
    ([], otherLinks) -> do -- no link to env yet
      putMVar gLinks ((env,retrylink gDefaultLink):otherLinks)
      forkIO (lifeCheck env) -- start new life check loop
      return ()
    ([(e, link)], otherLinks) -> do -- existing link, modify link only
      putMVar gLinks ((e, retrylink link):otherLinks)
    _ -> putMVar gLinks links -- internal error
  where retrylink l = foldr insertRetryLink l idVRs

insertRetryLink :: ValidRemVal -> AutoLink -> AutoLink
insertRetryLink (_, (Just (_, rVar), _)) link@AutoLink{autoRetry=rVars} = 
                -- duplicate retryVars in different TVars in same env possible
                         link{autoRetry=rVar:rVars}
insertRetryLink _ link = link

deleteRetryLinks ::  EnvAddr -> [ValidRemVal] -> IO ()
deleteRetryLinks env idVRs = do -- env mapped over other processes in trans
  links <- takeMVar gLinks
  case Data.List.partition ((== env).fst) links of
    ([], otherLinks) -> do -- no link to env
      putMVar gLinks otherLinks -- internal error
    ([(e, link)], otherLinks) -> -- existing link, modify link only
      putMVar gLinks ((e, foldr deleteRetryLink link idVRs):otherLinks)
    _ -> putMVar gLinks links -- internal error

deleteRetryLink :: ValidRemVal -> AutoLink -> AutoLink
deleteRetryLink (_, (Just (_, rVar), _)) link@AutoLink{autoRetry=rVars} = 
  -- filter also possible duplicate retryVars in different TVars in same env
                         link{autoRetry = P.filter (/= rVar) rVars}
deleteRetryLink _ link = link

lifeCheck :: EnvAddr -> IO ()
lifeCheck env = CE.catch (do
  links <- readMVar gLinks
  case P.lookup env links of
    Nothing -> do
               debugStrLn9 ("lifeCheck Nothing: "++show env)
               return () -- no ping for env
    Just link | autoTrans link > 0 -> do
                                      debugStrLn9 ("lifeCheck trans "++show env)
                                      ping
                                      threadDelay (gMegaMuSec * gTransChkIntv) 
                                      lifeCheck env
              | autoRetry link /= [] -> do
                                      debugStrLn9 ("lifeCheck retry "++show env)
                                      ping
                                      threadDelay (gMegaMuSec * gRetryChkIntv) 
                                      lifeCheck env
              | otherwise -> do -- last ping
                             ping
                             debugStrLn9 ("lifeCheck -------: "++show env)
                             deleteLink
  )(\(e::SomeException) -> do
          debugStrLn9 ("!!! lifeCheck recovery" ++ show e)
          aStat DRP
          recoverBrokenReactiveTrans env
          recoverBrokenInactiveTrans env
          deleteLink
   )
  where 
    ping = remPutMsg env RemLifeCheck
    deleteLink = modifyMVar_ gLinks (return.P.filter ((/= env).fst))

recoverBrokenReactiveTrans :: EnvAddr -> IO ()
recoverBrokenReactiveTrans env = do
  debugStrLn1 ("<<< !!!   recoverBrokenReactiveTrans: "++show env)
  conts <- readMVar gDistTransCont
  let brokenRemTrans = P.filter ((== env) . fst . fst) conts
  mapM_ ((flip writeChan Err).snd) brokenRemTrans
  debugStrLn1 ("<<<   recoverBrokenReactiveTrans "++show brokenRemTrans)

recoverBrokenInactiveTrans :: EnvAddr -> IO ()
recoverBrokenInactiveTrans env = do
  debugStrLn1 ("<<< !!! recoverBrokenInactiveTrans env: "++show env)
  links <- readMVar gLinks
  case P.lookup env links of
    Nothing                            -> return ()
    Just AutoLink{autoRetry=retryVars} -> do
      debugStrLn1 ("<<< recoverBrokenInactiveTrans retryVars: "++show retryVars)
      mapM_ coreResume retryVars -- resume retryVars (deadlock from broken link)
coreResume :: RetryVar -> IO ()
coreResume (RetryVar retryMVar _) = resumeRetryVarAct retryMVar
coreResume (LinkRetryVar (VarLink env retVarId)) = CE.catch (
  remPutMsg env (RemResume [retVarId])
-- ignore errors in coreResume, dead process proxy waking up other dead process
  ) (\(_::SomeException) -> return ())

--------------------
-- Initialization --
--------------------

-- |'startDist' enables inter process communication and exception handling and
-- then executes the given main function 
startDist :: IO ()  -- ^application main function to be executed. Each main 
                    -- function in the distributed system has to be wrapped
                    -- in a 'startDist' call  
             -> IO ()
startDist nodeMain = CE.catch (do
  serverPid <- startNameService -- may be removed
  threadDelay gMegaMuSec
  installHandler sigPIPE Ignore Nothing -- no UNIX signal on closed sockets
  time <- getProcessTimes
  debugStrLn1 ("listenOn: "++show (elapsedTime time))
  seq gMyEnv (forkIO (nodeReceiver gMySocket))
  --                  >> forkIO timedDebugger
  nodeMain
  swapMVar gLinks [] -- delete all links to enable finalizer GC
  performGC          -- activate all pending finalizers
  terminateProcess serverPid -- may be removed
  debugStrLn1 ("terminate NameService")
  )(propagateEx "startDist")

startNameService :: IO ProcessHandle
startNameService = do
  debugStrLn1 ("startNameService")
  runCommand "NameServer"
