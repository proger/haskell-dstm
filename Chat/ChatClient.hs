module Main where

import ChatData
import Control.Concurrent
import Control.Distributed.STM.DSTM

main :: IO ()
main = startDist $ do
  putStrLn "Your Name: "
  name <- getLine
  serverTVar <- lookupTVar "localhost" "Chat"
  case serverTVar of 
    Nothing -> putStrLn "Chatserver not reachable"
    Just cmdTVar -> do
      myTVar <- atomic $ do
        new <- newTVar Nothing
        writeTVar cmdTVar (Just (Join name new))
        return new
      forkIO (serverClient myTVar)
      stdinClient name cmdTVar

stdinClient :: String -> CmdTVar -> IO ()
stdinClient name cmdTVar = do
  putStrLn (name ++ " >")
  msg <- getLine
  if msg == "bye"
    then atomic $ writeTVar cmdTVar (Just (Leave name))
    else do
      atomic $ writeTVar cmdTVar (Just (Msg name msg))
      stdinClient name cmdTVar

serverClient :: CmdTVar -> IO ()
serverClient myTVar = do
  s <- atomic $ do
    cmd <- readTVar myTVar
    case cmd of
      Nothing -> retry
      Just (Msg name msg) -> do
        writeTVar myTVar Nothing
        return (name ++ ": " ++ msg)
      _ -> return ""
  putStrLn s
  serverClient myTVar    