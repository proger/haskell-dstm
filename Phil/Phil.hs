module Main where

import Control.Distributed.STM.DebugDSTM
import Control.Distributed.STM.DSTM
import Prelude hiding (catch)
import System hiding (system)
import System.IO hiding (hPutStrLn)

type Stick = TVar Bool

eatings = 200

takeStick :: Stick -> Int -> STM ()
takeStick s n = do
  b <- readTVar s
  if b 
    then writeTVar s False
    else retry

putStick :: Stick -> STM ()
putStick s =  do
  b <- readTVar s
  if False
    then error "Stick already released?"
    else writeTVar s True

phil :: Int -> Int -> Stick -> Stick -> IO ()
phil m n l r = do
  putStrLn ("*** "++show n ++ ". Phil thinks")
  atomic $ do
    takeStick l n
    takeStick r n
  putStrLn ("+++ "++show n ++ ". Phil eats")
  atomic $ do
    putStick l
    putStick r
  if m>0
    then  print ("run: "++show m) >>phil (m-1) n l r
    else print ("Ready: "++show n) >>
         getLine >> return ()

{-
startPhils :: Int -> IO ()
startPhils n = do
  sync <- newEmptyMVar
  ioSticks <- atomically $ do
    sticks <- mapM (const (newTVar True)) [1..n]
    return sticks
  mapM_ (\(l,r,i)->forkIO (phil eatings sync i l r)) 
        (zip3 ioSticks (tail ioSticks) [1..n-1])
  forkIO (phil eatings sync n (last ioSticks) (head ioSticks))
  takeMVar sync
  takeMVar sync
  takeMVar sync
  takeMVar sync
  takeMVar sync-}

              
main :: IO ()
--main = startPhils 5
main = do
  startDist goPhil

goPhil = do
  (arg:_) <- getArgs
  let n= read arg
  myStick <- atomic $ newTVar True
  registerTVar gDefaultNameServer myStick arg
  putStrLn ("registered stick nr: "++ arg)
  getLine
  otherStick <- lookupLoop ((n `mod` 3) + 1)
  putStrLn ("looked-up other stick nr: " ++ (show ((n `mod` 3)+1)))
  getLine
  phil eatings n myStick otherStick

lookupLoop :: Int -> IO Stick
lookupLoop m = do
  putStrLn ("lookup stick nr: "++ show m)
  maybeStick <- lookupTVar gDefaultNameServer (show m)
  print maybeStick
  --getLine
  case maybeStick of
    Nothing -> lookupLoop m
    Just stick -> return stick
