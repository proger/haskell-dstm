{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.STM (STM (STM), STMState (STMState), 
           STMResult (Retry, Success, Exception), 
           runSTM, stmId, stmValid, stmCommit, stmRetryVar) where

import Control.Distributed.STM.RetryVar
import Control.Distributed.STM.TVar
import Control.Exception as CE (SomeException)


-------------------
-- The STM monad --
-------------------

-- |A monad supporting atomic memory transactions
newtype STM a = STM (STMState -> IO (STMResult a))

instance Monad STM where
  -- (>>=) :: STM a -> (a -> STM b) -> STM b
  (STM tr1)  >>= k = STM (\state -> do
                          stmRes <- tr1 state
                          case stmRes of
                            Success newState v ->
                               let (STM tr2) = k v in
                                 tr2 newState
                            Retry newState -> return (Retry newState)
			    Exception newState e -> return (Exception newState e)
                       )
  -- return :: a -> STM a
  return x      = STM (\state -> return (Success state x))

data STMState = STMState {stmId       :: TransID,
                          stmRetryVar :: RetryVar,
		          stmValid    :: [ValidLog],
		          stmCommit   :: [CommitLog]}

data STMResult a = Retry STMState
		 | Success STMState a
		 | Exception STMState CE.SomeException

runSTM :: STM a -> STMState -> IO (STMResult a)
runSTM (STM stm) state = stm state

