module Main where

import ChatData
import Control.Distributed.STM.DebugDSTM
import Control.Distributed.STM.DSTM
import Control.Exception as CE

main :: IO ()
main = startDist $ do
  inVar <- atomic $ newTVar Nothing
  registerTVar "localhost" inVar "Chat"
  chatServer inVar []

chatServer :: CmdTVar -> [(String, CmdTVar)] -> IO ()
chatServer inCmd dict = CE.catch (do
  newDict <- atomic $ do
    cmd <- readTVar inCmd
    case cmd of
      Nothing -> do
                 mapM_ (readTVar.snd) dict
                 retry
      Just serverCmd -> do
        writeTVar inCmd Nothing
        case serverCmd of
          Join name msgVar -> do
             proc $ putStrLn (name++" joined the chat")
             mapM_ (flip writeTVar msg.snd) dict
             return ((name,msgVar):dict)
             where msg = Just (Msg name " joined")
          Msg _ _ -> do
             proc $ putStrLn "Message forwarded"
             mapM_ (flip writeTVar cmd.snd) dict
	     return dict
          Leave name -> do
             proc $ putStrLn (name++" left the chat")
	     mapM_ (flip writeTVar msg.snd) dic
             return dic
             where msg = Just (Msg name " left")
                   dic = filter ((/=name).fst) dict
  chatServer inCmd newDict
  )(\e -> print "Dropped Client" >> chatServer inCmd (removeErrDict e dict))

removeErrDict :: SomeDistTVarException -> [(String, CmdTVar)] 
                 -> [(String, CmdTVar)]
removeErrDict e dict = [d | d <- dict, not (isDistErrTVar e (snd d))]