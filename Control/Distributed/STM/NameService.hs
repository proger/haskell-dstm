{-# OPTIONS_GHC -XScopedTypeVariables #-}
{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.NameService 
                   (nameService, gDefaultNameServer,
                    registerTVar, deregisterTVar, lookupTVar) where

import qualified Control.Exception as CE
import Control.Distributed.STM.DebugBase
import Control.Distributed.STM.Dist
import Control.Distributed.STM.EnvAddr
import Control.Distributed.STM.TVar
import Network
import Prelude as P hiding (catch, putStr, putStrLn)
import System.IO

---------------
-- Messaging --
---------------

data NameServerMsg = NameServerPing
                   | NameServerReg    String VarLink
		   | NameServerUnReg  String
		   | NameServerLookup String
  deriving (Show, Read)

type TVarDict = [(String, VarLink)]

-----------------
-- Post Office --
-----------------

putNameServerLn :: Show a => String -> a -> IO ()
putNameServerLn nameServer msg = do
  debugStrLn0 ("--> putNameServerLn: "++nameServer++" msg: "++(show msg))
  h <- connectTo nameServer (PortNumber gNameServerPort) 
  sendTCP msg h
  hClose h

getNameServerLn :: Show a => String -> a -> IO String
getNameServerLn nameServer msg = do
  debugStrLn0 ("<-- getNameServerLn: "++nameServer++" msg: "++(show msg))
  h <- connectTo nameServer (PortNumber gNameServerPort) 
  answer <- recvTCP msg h
  hClose h
  return answer

------------------
-- Name Service --
------------------

-- |The default name server for the process running the main function. 
-- Usually it is @localhost@.
gDefaultNameServer :: String
gDefaultNameServer = "localhost"

nameService :: String -> IO ()
nameService server = CE.catch (putNameServerLn server NameServerPing)
  (\(e::CE.SomeException) -> do -- no name server yet -> start one per node
         debugStrLn0 ("nameService: start name server: "++ show e)
         listenOn (PortNumber gNameServerPort) >>= readNameServerMsg [])

readNameServerMsg :: TVarDict -> Socket -> IO ()
readNameServerMsg tVarDict s = do
  debugStrLn0 ("nameService: readNameServerMsg: "++ show s)
  (h, _, _) <- accept s
  str <- hGetLine h
  newTable <- case reads str of
    ((msg,_):_) -> handleNameServerMsg h msg tVarDict
    _           -> return tVarDict -- internal error
  hClose h
  readNameServerMsg newTable s

handleNameServerMsg :: Handle -> NameServerMsg -> TVarDict -> IO TVarDict
handleNameServerMsg h msg tVarDict =
  case msg of
    NameServerPing -> return tVarDict
    NameServerReg name tVar -> do
         debugStrLn0 ("Registered: "++(show name))
         return $ (name, tVar):filter ((name/=).fst) tVarDict
    NameServerUnReg name -> do
         debugStrLn0 ("Unregistered: "++(show name))
         return $ filter ((name/=) . fst) tVarDict
    NameServerLookup name -> do
         debugStrLn0 ("Lookup: "++(show name))
         hPutStrLn h (show (lookup name tVarDict))
         return tVarDict

-- |'registerTVar' @server tVar name@ registers @tVar@ with @name@ onto @server@
registerTVar :: Dist a => String -> TVar a -> String -> IO ()
registerTVar server tVar name = CE.catch (do
  debugStrLn0 ("registerTVar: " ++ show tVar ++ " " ++ name)
  putNameServerLn server (NameServerReg name (tVarToLink tVar))
  regTVars gMyEnv tVar -- generate actions for exported tVar
  )(propagateEx "registerTVar")

-- |'deregisterTVar' @server name@ removes @name@ from @server@
deregisterTVar :: String -> String -> IO ()
deregisterTVar server name = CE.catch (do
  debugStrLn0 ("deregisterTVar: " ++ show name)
  putNameServerLn server (NameServerUnReg name)
  )(propagateEx "deregisterTVar")

-- |'lookupTVar' @server name@ returns ('Just' @tVar@) if a @tVar@ registration
-- of @name@ exists on @server@, 'Nothing' otherwise.
lookupTVar :: forall a . Dist a => String -> String -> IO (Maybe (TVar a))
lookupTVar server name = CE.catch (do
  debugStrLn0 ("lookupTVar: " ++ server ++ " , " ++ name)
  answer <- getNameServerLn server (NameServerLookup name)
  debugStrLn0 ("lookupTVar: " ++ show answer)
  case reads answer of
    ((Just link,_):_) -> do
      let tVar::TVar a = LinkTVar link
      finTVars tVar
      return $ Just tVar
    _ -> return Nothing
  )(propagateEx "lookupTVar")
