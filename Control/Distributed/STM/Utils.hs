{-# OPTIONS_HADDOCK hide #-}

module Control.Distributed.STM.Utils ((>>+), (+>>), insertWith, showJustify,
                                      replaceListElems, updateListElems) where

(>>+) :: IO Bool -> IO Bool -> IO Bool
a1 >>+ a2 = do
  b <- a1
  if b then a2
       else return False

(+>>) :: Bool -> IO Bool -> IO Bool
b +>> a = if b then a else return False

{- insertWith key new update keyvalues
inserts sorted (key, new) into keyvalues
if key exists calls (update new old) at key -}

insertWith :: Ord a => (b -> b -> b) -> a -> b ->  [(a, b)] -> [(a, b)]
insertWith _ key new [] = [(key, new)]
insertWith upd key new ((k, old) : kvs)
     | key == k  = (k, upd new old) : kvs
     | key > k   = (k, old) : insertWith upd key new kvs
     | otherwise = (key, new) : (k, old) : kvs

replaceListElems :: Eq a => b -> a -> [(a,b)] -> [(a,b)]
replaceListElems val key []                      = [(key, val)]
replaceListElems val key ((k,v):kvs) | key == k  = (k, val) : kvs
                                     | otherwise = (k,v)
                                                   :replaceListElems val key kvs

updateListElems :: Eq a => (b -> b) -> b -> a -> [(a,b)] -> [(a,b)]
updateListElems _ val key []                     = [(key, val)]
updateListElems f v0 key ((k,v):kvs) | key == k  = (k, f v) : kvs
                                     | otherwise = (k,v)
                                                   :updateListElems f v0 key kvs

showJustify :: Show a => a -> Int -> String
showJustify i len = let s = show i in (replicate (len - length s) ' ') ++ s
