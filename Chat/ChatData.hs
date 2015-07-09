module ChatData where

import Control.Distributed.STM.DSTM

type CmdTVar = TVar (Maybe ServerCmd)

data ServerCmd = Join String CmdTVar
	       | Msg String String
	       | Leave String
  deriving (Show,Read)

instance Dist ServerCmd where
  finTVars (Join _ cmd) = finTVars cmd 
  finTVars _ = return ()
 
  regTVars env (Join _ cmd) = regTVars env cmd
  regTVars _ _ = return ()
