{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.Dist (Dist, regTVars, finTVars) where

import Control.Distributed.STM.EnvAddr
import Prelude as P

---------------------------
-- Class Definition Dist --
---------------------------

-- |The class 'Dist' defines the distribution property of 'TVar' values. Any 
-- TVar value must implement class 'Dist'. All basic data types exported by
-- the Prelude are instances of 'Dist', and 'Dist' may be derived for any data
-- type whose constituents are also instances of 'Dist'. Any custom-typed TVar
-- value type should implement 'finTVars' and 'regTVars' to do nothing and 
-- return '()'.
--
-- Note that 'finTVars' and 'regTVars' should never be called by the application
-- itself!
class (Show a, Read a) => Dist a where
  -- |Do not call regTVars yourself!
  --
  -- regTVars registers all TVars within @a@ with a host TVar link count before 
  -- the TVars in @a@ are sent to remote nodes
  regTVars :: EnvAddr -> a -> IO () -- create link in env to remote TVars in a
  -- |Do not call finTVars yourself!
  --
  -- finTVars installs finalizers at all link TVars in @a@ which send messages
  -- to their host TVars to remove them from the host TVar link count after the
  -- link TVars have been garbage collected
  finTVars :: a -> IO ()       -- finalizer to delete link to remote TVars in a


reg2 :: (Dist a, Dist b) => EnvAddr -> a -> b -> IO ()
reg2 env x y = regTVars env x >> regTVars env y

reg3 :: (Dist a, Dist b, Dist c) => EnvAddr -> a -> b -> c -> IO ()
reg3 env x y z = regTVars env x >> regTVars env y >> regTVars env z

reg4 :: (Dist a, Dist b, Dist c, Dist d) => EnvAddr ->a -> b -> c -> d -> IO ()
reg4 e w x y z = regTVars e w >> regTVars e x >> regTVars e y >> regTVars e z

fin2 :: (Dist a, Dist b) => a -> b -> IO ()
fin2 x y = finTVars x >> finTVars y

fin3 :: (Dist a, Dist b, Dist c) => a -> b -> c -> IO ()
fin3 x y z = finTVars x >> finTVars y >> finTVars z

fin4 :: (Dist a, Dist b, Dist c, Dist d) => a -> b -> c -> d -> IO ()
fin4 w x y z = finTVars w >> finTVars x >> finTVars y >> finTVars z

instance Dist a => Dist [a] where
  finTVars [] = return ()
  finTVars (x:xs) = fin2 x xs

  regTVars _ [] = return ()
  regTVars env (x:xs) = reg2 env x xs

instance Dist a => Dist (Maybe a) where
  finTVars (Nothing) = return ()
  finTVars (Just x) = finTVars x

  regTVars _ (Nothing) = return ()
  regTVars env (Just x) = regTVars env x

instance (Dist a, Dist b) => Dist (Either a b) where
  finTVars (Left x) = finTVars x
  finTVars (Right x) = finTVars x

  regTVars env (Left x) = regTVars env x
  regTVars env (Right x) = regTVars env x

instance Dist Bool where
  finTVars _ = return ()
  regTVars _ _ = return ()

instance Dist Int where
  finTVars _ = return ()
  regTVars _ _ = return ()

instance Dist Integer where
  finTVars _ = return ()
  regTVars _ _ = return ()

instance Dist Char where
  finTVars _ = return ()
  regTVars _ _ = return ()

instance Dist Float where
  finTVars _ = return ()
  regTVars _ _ = return ()

instance Dist () where
  finTVars () = return ()
  regTVars _ () = return ()

instance (Dist a, Dist b) => Dist (a,b) where
  finTVars (x, y) = fin2 x y
  regTVars env (x, y) = reg2 env x y

instance (Dist a, Dist b, Dist c) => Dist (a,b,c) where
  finTVars (x, y, z) = fin3 x y z
  regTVars env (x, y, z) = reg3 env x y z

instance (Dist a, Dist b, Dist c, Dist d) => Dist (a,b,c,d) where
  finTVars (w, x, y, z) = fin4 w x y z
  regTVars env (w, x, y, z) = reg4 env w x y z
