{-# OPTIONS_GHC -XScopedTypeVariables -XDeriveDataTypeable #-}
{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.EnvAddr (ID, STMID, VarID, VersionID, uniqueId,
                EnvAddr, gMyEnv, gMySocket, VarLink (VarLink),
                connectToRetryEnv, connectToEnvHold, connectAtomicToEnv,
                SomeDistTVarException (PropagateDistTVarFail, CommunicationFail,
                NodeConnectionFail), propagateEx, distTVarExEnv,
                sendTCP, recvTCP, aStat, printTCPStat,
                RemMsgIdx (ATM,INV,STT,CTT,ENC,RES,AEA,DEA,LFC,DRP,RDT),
                gNameServerPort, connectToNameServer) where

import Control.Concurrent
import Control.Exception
import Data.Dynamic
import Control.Distributed.STM.DebugBase
import Control.Distributed.STM.Utils
import Network
import Network.BSD
import Network.Socket
import Maybe
import Prelude as P hiding (catch, putStr, putStrLn)
import System.IO
import System.IO.Unsafe

-----------------
-- Identifiers --
-----------------

type ID         = Integer
type STMID      = ID
type VarID      = ID
type VersionID  = ID

---------------------------------------
-- Unique IDs from common pool for   --
-- STM, TVar, TVar-Version, RetryVar --
---------------------------------------

gInitID :: ID
gInitID = 1

gFreshIds :: MVar [ID]
{-# NOINLINE gFreshIds #-}
gFreshIds  = unsafePerformIO (newMVar [gInitID..])

uniqueId :: IO ID
uniqueId = modifyMVar gFreshIds (\(h:t) -> return (t,h))

freeGlobalId :: ID -> IO ()
freeGlobalId n = modifyMVar_ gFreshIds (return . (n:))

-------------------------
-- Network Environment --
-------------------------

gNameServerPort :: PortNumber
gNameServerPort = 60000

gServerPort :: PortNumber
gServerPort = 60001

freshPort :: PortNumber -> IO (PortID, Socket)
freshPort p = catch (do
                     s <- listenOn (PortNumber p)
                     return (PortNumber p,s)
                     )(\(_::SomeException) -> freshPort (p+1))

type IPAddr  = String

data EnvAddr = EnvAddr PortID IPAddr
  deriving (Show, Read, Eq, Ord)

data VarLink = VarLink EnvAddr -- host TVar node
                       VarID   -- TVar Id on host TVar node
  deriving (Eq, Show, Read)

instance Show PortID where
 show (PortNumber p) = show p
 show _      = "Show PortID: otherPort" -- internal error

instance Read PortID where
 readsPrec _ str0 =
     case reads str0 of
       ((p,str2):_) -> [(PortNumber (fromInteger p),str2)]
       o -> error ("error readsPrec PortID" ++ show o)

instance Eq PortID where
 (PortNumber p1)==(PortNumber p2) = p1==p2
 _ == _ = False -- error

instance Ord PortID where
 (PortNumber p1)<=(PortNumber p2) = p1<=p2
 _ <= _ = False -- error

gMyIpAddr :: IPAddr
{-# NOINLINE gMyIpAddr #-}
gMyIpAddr = unsafePerformIO $ do
  hName <- getHostName
  hEntry <- getHostByName hName
  inet_ntoa (hostAddress hEntry)

gMyPort :: PortID
{-# NOINLINE gMyPort #-}
gMySocket :: Socket
{-# NOINLINE gMySocket #-}
(gMyPort,gMySocket) = unsafePerformIO $ do
  (port,sock) <- freshPort gServerPort
  return (port,sock)

gMyEnv :: EnvAddr -- initialize environment
gMyEnv = (EnvAddr $! gMyPort) $! gMyIpAddr

nameServerEnv :: String -> EnvAddr
nameServerEnv = EnvAddr (PortNumber gNameServerPort)

-----------------
-- Post Office --
-----------------

type TCPSndConn = (EnvAddr, Handle)

gTCPSndConn :: MVar [TCPSndConn]
{-# NOINLINE gTCPSndConn #-}
gTCPSndConn = unsafePerformIO (newMVar [])

lookupTCPSndEnv :: Handle -> IO EnvAddr
lookupTCPSndEnv h = do
  conns <- readMVar gTCPSndConn
  debugStrLn5 ("lookupTCPSndEnv: "++show conns++"\n"++show h++"\n"++show (lookupSndEnv h conns))
  return (fromJust (lookupSndEnv h conns))

lookupSndEnv :: Handle -> [TCPSndConn] -> Maybe EnvAddr
lookupSndEnv _ [] = Nothing
lookupSndEnv key ((env, h):conns) | key == h = Just env
                                  | otherwise = lookupSndEnv key conns

connectToEnvHold' :: EnvAddr -> IO Handle
connectToEnvHold' env = catch (do
  sndConns <- readMVar gTCPSndConn
  case P.lookup env sndConns of
    Just h  -> return h
    Nothing -> do -- no tcp yet, establish (one and only) unidir. conn.
      h <- connectToEnv' env
      modifyMVar_ gTCPSndConn (return . ((env, h):))
      return h
  )(propagateEx "connectToEnvHold'")

type TCPRecConn = (EnvAddr, [Handle])

gTCPRecConn :: MVar [TCPRecConn]
{-# NOINLINE gTCPRecConn #-}
gTCPRecConn = unsafePerformIO (newMVar [])

gTCPTotalRecConn :: MVar [TCPRecConn]
{-# NOINLINE gTCPTotalRecConn #-}
gTCPTotalRecConn = unsafePerformIO (newMVar [])

-- all open connections to determine env in case of broken connection
lookupTCPRecEnv :: Handle -> IO EnvAddr
lookupTCPRecEnv h = do
  conns <- readMVar gTCPTotalRecConn
  debugStrLn5 ("lookupTCPRecEnv: "++show conns++"\n"++show h++"\n"++show (lookupRecEnv h conns))
  case lookupRecEnv h conns of
    Nothing  -> return gMyEnv -- internal error
    Just env -> return env

lookupRecEnv :: Handle -> [TCPRecConn] -> Maybe EnvAddr
lookupRecEnv _ []                = Nothing
lookupRecEnv key ((_, []):conns) = lookupRecEnv key conns
lookupRecEnv key ((env, (h:hs)):conns) 
                      | key == h  = Just env
                      | otherwise = lookupRecEnv key ((env, hs):conns)

connectAtomicToEnv' :: EnvAddr -> (Handle -> IO String) -> IO String
connectAtomicToEnv' env tcp = do
  debugStrLn3 ("connectAtomicToEnv ->: " ++ show env)
  h <- allocTcpHandle env
  debugStrLn3 ("connectAtomicToEnv -: " ++ show h)
  answer <- tcp h -- execute 2-way tcp comm. atomically w/rspct to handle
  debugStrLn3 ("connectAtomicToEnv --: " ++ show answer)
  freeTcpHandle h env
  debugStrLn3 ("connectAtomicToEnv <-: ")
  return answer

freeTcpHandle :: Handle -> EnvAddr -> IO ()
freeTcpHandle h env = do
  tcpConns <- takeMVar gTCPRecConn
  case P.lookup env tcpConns of
    Just hs -> putMVar gTCPRecConn (replaceListElems (h:hs) env tcpConns)
    Nothing -> putMVar gTCPRecConn ((env,[h]):tcpConns)


allocTcpHandle :: EnvAddr -> IO Handle
allocTcpHandle env = catch (do
  conns <- takeMVar gTCPRecConn
  case P.lookup env conns of
    Just []     -> do -- no tcp available -> get new tcp
                   putMVar gTCPRecConn conns
                   h <- connectToEnv' env
                   modifyMVar_ gTCPTotalRecConn (return . (updateListElems (h:) [h] env))
                   return h
    Just (h:hs) -> do -- get head of available tcp
                   putMVar gTCPRecConn (replaceListElems hs env conns)
                   return h
    Nothing     -> do -- no tcp yet -> get first new tcp
                   putMVar gTCPRecConn conns
                   h <- connectToEnv' env
                   modifyMVar_ gTCPTotalRecConn (return . ((env,[h]):))
                   return h
  )(propagateEx "allocTcpHandle")

connectToEnv' :: EnvAddr -> IO Handle
connectToEnv' env@(EnvAddr pid ip) = catch (do 
  h <- connectTo ip pid
  hSetBuffering h LineBuffering
  return h
  )(\e -> debugStrLn1 ("connectToEnv' " ++ show e) >> throw (NodeConnectionFail "connectToEnv' " env e))

connectToNameServer' :: String -> IO Handle
connectToNameServer' = connectToEnv'.nameServerEnv

--------------------
-- TCP Statistics --
--------------------

data RemMsgIdx = ATM|INV|STT|CTT|ENC|RES|AEA|DEA|LFC|DRP|RDT
  deriving (Eq, Ord, Show)

type STMMsgStat = (RemMsgIdx, Int)

gSTMMsgStat :: MVar [STMMsgStat]
{-# NOINLINE gSTMMsgStat #-}
gSTMMsgStat = unsafePerformIO (newMVar ((ATM, 0):(INV,0):[]))

aStat :: RemMsgIdx -> IO ()
aStat msgIdx = modifyMVar_ gSTMMsgStat (return . (insertWith (\_ x ->x+1)) msgIdx 1)

data TCPStat = TCPStat {
                conEnv      :: Int,
                conExclEnv  :: Int,
                conRetryVar :: Int,
                conNS       :: Int
                }

gTCPStat :: MVar TCPStat
{-# NOINLINE gTCPStat #-}
gTCPStat = unsafePerformIO (newMVar (TCPStat{conEnv      = 0, 
                                             conExclEnv  = 0,
                                             conRetryVar = 0,
                                             conNS       = 0}))

connectToEnvHold :: EnvAddr -> IO Handle
connectToEnvHold env = do
  modifyMVar_ gTCPStat (\ips -> return ips{conEnv=(conEnv ips)+1})
  connectToEnvHold' env

connectAtomicToEnv :: EnvAddr -> (Handle -> IO String) -> IO String
connectAtomicToEnv env tcp = do
  modifyMVar_ gTCPStat (\ips -> return ips{conExclEnv=(conExclEnv ips)+1})
  connectAtomicToEnv' env tcp

connectToRetryEnv :: EnvAddr -> IO Handle
connectToRetryEnv env = do
  modifyMVar_ gTCPStat (\ips -> return ips{conRetryVar=(conRetryVar ips)+1})
  connectToEnvHold' env

connectToNameServer :: String -> IO Handle
connectToNameServer ns = do
  modifyMVar_ gTCPStat (\ips -> return ips{conNS=(conNS ips)+1})
  connectToNameServer' ns

printTCPStat :: String -> IO ()
printTCPStat s = do
  ips <- readMVar gTCPStat
  sndConns <- readMVar gTCPSndConn
  recConns <- readMVar gTCPRecConn
  msgs <- readMVar gSTMMsgStat
  printStat s ips sndConns recConns msgs

printStat :: String -> TCPStat -> [TCPSndConn] -> [TCPRecConn] -> [STMMsgStat] 
             -> IO ()
printStat s ips sndConns recConns msgs = do
  let l = foldr (+) 1 (map (length.snd) recConns)
  takeMVar gDebugLock
  putStrLn s
  putStrLn ("--> TCP Verbindungen: " ++ showJustify (length sndConns) 5)
  putStrLn ("<-> TCP Verbindungen: " ++ showJustify l 5)
  putStrLn ("--> TCP Nachrichten:  " ++ showJustify (conEnv ips) 5)
  putStrLn ("<-> TCP Nachrichten:  " ++ showJustify (conExclEnv ips) 5)
  putStrLn ("|-> TCP Nachrichten:  " ++ showJustify (conRetryVar ips) 5)
  printMsgs msgs
  putMVar gDebugLock ()

printMsgs :: [STMMsgStat] -> IO ()
printMsgs [] = return ()
printMsgs msgs = do
  putStrLn (">-> Transaktionen:    " ++ (showJustify.snd.head) msgs 5)
  putStrLn ("-X- Transaktionen:    " ++ (showJustify.snd.head.tail) msgs 5)
  putStrLn ("<-- TCP Nachrichten:  " ++ showJustify (foldr (+) 0 (map snd ((tail.tail) msgs))) 5 ++ "\n")
  mapM_ printMsg ((tail.tail) msgs)

printMsg :: (RemMsgIdx, Int) -> IO ()
printMsg (msg, count) = putStrLn (show msg ++ ": " ++ showJustify count 5)

----------------
-- Robustness --
----------------

-- |'SomeDistTVarException' is the abstract exception type which is thrown
-- by the DSTM library when either 'readTVar' or 'writeTVar' is called on an
-- unreachable TVar.
-- A TVar becomes unreachable when the process hosting the TVar becomes
-- unreachable. 
-- An atomic transaction using a TVar which becomes unreachable during the 
-- execution of 'atomic' may either execute completely (without the unreachable
-- TVar(s)) or execute not at all depending on transaction states. In either
-- case an exception of type 'SomeDistTVarException' is raised.
data SomeDistTVarException = PropagateDistTVarFail String SomeDistTVarException
                           | CommunicationFail String EnvAddr SomeException
                           | NodeConnectionFail String EnvAddr SomeException
  deriving Typeable

instance Exception SomeDistTVarException

instance Show SomeDistTVarException where
  show (PropagateDistTVarFail loc ex) = 
    "PropagateDistTVarFail " ++ loc ++ "\n" ++ show ex
  show (CommunicationFail loc env ex)  = formEx "CommunicationFail" loc env ex
  show (NodeConnectionFail loc env ex) = formEx "NodeConnectionFail" loc env ex

formEx :: String -> String -> EnvAddr -> SomeException -> String
formEx err loc env cause = "Message : " ++ err
                        ++ "\nLocation: " ++ loc
                        ++ "\nProcess : " ++ show env
                        ++ "\nSysError: " ++ show cause ++ "\n"

propagateEx :: String -> SomeDistTVarException -> IO a
propagateEx loc e = throw (PropagateDistTVarFail (loc ++ " -> ") e)

distTVarExEnv :: SomeDistTVarException -> EnvAddr
distTVarExEnv eDist = case eDist of
  (PropagateDistTVarFail _ e)  -> distTVarExEnv e
  (CommunicationFail _ env _)  -> env
  (NodeConnectionFail _ env _) -> env

--------------------------
-- Robust Remote Access --
--------------------------

gSendLock :: MVar ()
{-# NOINLINE gSendLock #-}
gSendLock  = unsafePerformIO (newMVar ())

sendTCP :: Show a => a -> Handle -> IO ()
sendTCP msg h = catch (do
  takeMVar gSendLock
  hPutStrLn h (show msg) 
  hFlush h
  putMVar gSendLock ()
  )(\e -> do
    debugStrLn1 $ "sendTCP " ++ show e
    putMVar gSendLock ()
    env <- lookupTCPSndEnv h
    throw (CommunicationFail ("sendTCP: " ++ show msg ++ show h) env e))

recvTCP :: Show a => a -> Handle -> IO String
recvTCP msg h = catch (do 
  hPutStrLn h (show msg) 
  hFlush h 
  hGetLine h
  )(\e -> do
    debugStrLn1 $ "recvTCP " ++ show e
    env <- lookupTCPRecEnv h
    throw (CommunicationFail ("recvTCP: " ++ show msg ++ show h) env e))
