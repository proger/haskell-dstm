{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.DebugBase
                 (debugStrLn0,debugStrLn1,debugStrLn2,debugStrLn3,debugStrLn4,
                  debugStrLn5,debugStrLn6,debugStrLn7,debugStrLn8,debugStrLn9,
                  gDebugLock, startGDebug, stopGDebug, gDebugStrLn,
                  newDebugMVar, inspectMVars, timedInspect) where

import Control.Concurrent
import Prelude
import System.IO
import System.IO.Unsafe

---------------------
-- Debugging Tools --
---------------------

debug0,debug1,debug2,debug3,debug4,debug5,debug6,debug7,debug8,debug9 :: Bool
debug0 = False -- name server
debug1 = False -- catch error
debug2 = False -- robustness
debug3 = False -- robust <-
debug4 = False -- tcp connection
debug5 = False -- robust ->
debug6 = False -- bomberman
debug7 = False -- atomic
debug8 = False -- 3 phase commit
debug9 = False -- life check

debugStrLn0 :: String -> IO ()
debugStrLn0 str = if debug0 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn1 :: String -> IO ()
debugStrLn1 str = if debug1 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn2 :: String -> IO ()
debugStrLn2 str = if debug2 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn3 :: String -> IO ()
debugStrLn3 str = if debug3 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn4 :: String -> IO ()
debugStrLn4 str = if debug4 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 
debugStrLn5 :: String -> IO ()
debugStrLn5 str = if debug5 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn6 :: String -> IO ()
debugStrLn6 str = if debug6 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn7 :: String -> IO ()
debugStrLn7 str = if debug7 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn8 :: String -> IO ()
debugStrLn8 str = if debug8 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

debugStrLn9 :: String -> IO ()
debugStrLn9 str = if debug9 then do
                                 myPid <- myThreadId
                                 takeMVar gDebugLock
                                 hPutStrLn stderr (show myPid++": "++str) 
                                 putMVar gDebugLock ()
                            else return () 

gDebug :: MVar Bool
{-# NOINLINE gDebug #-}
gDebug  = unsafePerformIO (newMVar False)

gDebugLock :: MVar ()
{-# NOINLINE gDebugLock #-}
gDebugLock  = unsafePerformIO (newMVar ())

gDebugStrLn :: String -> IO ()
gDebugStrLn str = do
  isDebug <- readMVar gDebug 
  if isDebug then do
                  myPid <- myThreadId
                  takeMVar gDebugLock
                  hPutStrLn stderr (show myPid++": "++str) 
                  putMVar gDebugLock ()
             else return ()

startGDebug :: IO ()
startGDebug = swapMVar gDebug True >> return ()
  
stopGDebug :: IO ()
stopGDebug = swapMVar gDebug False >> return ()

gMVarStates :: MVar (IO ())
{-# NOINLINE gMVarStates #-}
gMVarStates  = unsafePerformIO (newMVar (return ()))

newDebugMVar :: String -> a -> IO (MVar a)
newDebugMVar s var = do
  mVar <- newMVar var
  mVarStates <- takeMVar gMVarStates
  putMVar gMVarStates (do
           hPutStr stderr (s++" ")
           b <- isEmptyMVar mVar
           hPutStr stderr (if b then "empty !; " else "full; ")
           mVarStates)
  return mVar

inspectMVars :: String -> IO ()
inspectMVars s = do
  takeMVar gDebugLock
  hPutStrLn stderr s
  myPid <- myThreadId
  hPutStr stderr (show myPid++": ### MVar states >>>")
  mVarStates <- readMVar gMVarStates
  mVarStates
  hPutStrLn stderr ("<<< MVar states ###")
  putMVar gDebugLock ()

timedInspect :: IO ()
timedInspect = do
  if debug6 
    then do
         inspectMVars "### Timed Debugger ###"
         threadDelay (5 * 1000000)
         timedInspect
    else return ()

instance Show (IO a) where
  show _ = show "IO "

instance Show (MVar a) where
  show _ = show "MVar "

instance Show (Chan a) where
  show _ = show "Chan "

