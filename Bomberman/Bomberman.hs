{-# OPTIONS_GHC -XScopedTypeVariables -XDeriveDataTypeable #-}

module Main where

import Data.Char
import Control.Concurrent
import Control.Exception as CE hiding (catch)
import qualified Control.Exception as CE (catch)
import Control.Distributed.STM.DebugDSTM
import Control.Distributed.STM.DSTM
import Data.Maybe
import Network.BSD
import Prelude hiding (catch)
import System.Environment (getArgs)
import System.IO hiding (hPutStrLn)
import System.IO.Unsafe
import System.Posix

data Point = Point Int Int
  deriving (Eq, Show, Read)

instance Num Point where
  (Point x1 y1) + (Point x2 y2) = Point (x1 + x2) (y1 + y2)
  (Point x1 y1) - (Point x2 y2) = Point (x1 - x2) (y1 - y2)
  (Point x1 y1) * (Point x2 y2) = Point (x1 * x2) (y1 * y2)
  abs (Point x y)               = Point (abs x) (abs y)
  signum (Point x y)            = Point (signum x) (signum y)
  fromInteger x                 = Point (fromInteger x) (fromInteger x)

data Key = QKey
         | AKey | MKey | SKey
         | UpKey | DownKey | RightKey | LeftKey
         | SpaceKey
         | DebugKey
  deriving (Eq, Show, Read)

point :: Int -> Int -> Point
point x y = Point x y

cWallSymbol   :: Char;   cWallSymbol   = 'x'
cFSize        :: Int;    cFSize        = 10
cDelay        :: Int;    cDelay        = 2000000 --  2 s; 50 ms granularity
cInitLifes    :: Int;    cInitLifes    = 0
cInitPlayer   :: Point;  cInitPlayer   = point 2 2
pI            :: Point;  pI            = point 0 0
pR            :: Point;  pR            = point 1 0
pL            :: Point;  pL            = point (-1) 0
pU            :: Point;  pU            = point 0 (-1)
pD            :: Point;  pD            = point 0 1
fieldFile     :: String; fieldFile     = "./field.txt"
emptyImage    :: String; emptyImage    = "  "
wallImage     :: String; wallImage     = "W "
playerImage   :: String; playerImage   = "o "
slaveImage    :: String; slaveImage    = "@ "
bombImage     :: String; bombImage     = ". "
blastImage    :: String; blastImage    = "X "
esc           :: Char;   esc           = chr 27
csi           :: Char;   csi           = chr 91
upKey         :: Char;   upKey         = chr 65
downKey       :: Char;   downKey       = chr 66
rightKey      :: Char;   rightKey      = chr 67
leftKey       :: Char;   leftKey       = chr 68
aKeys         :: [Char]; aKeys         = ['a','A']
mKeys         :: [Char]; mKeys         = ['m','M']
sKeys         :: [Char]; sKeys         = ['s','S']
qKeys         :: [Char]; qKeys         = ['q','Q']
dKeys         :: [Char]; dKeys         = ['d','D']
spaceKey      :: Char;   spaceKey      = ' '
initKeys      :: [Char]; initKeys      = aKeys++mKeys++sKeys
ctrlKeys      :: [Char]; ctrlKeys      = (esc:spaceKey:qKeys)++dKeys
cursKeys      :: [Char]; cursKeys      = [upKey,downKey,rightKey,leftKey]
clearScreen   :: String; clearScreen   = (esc:csi:"2J")
home          :: String; home          = (esc:csi:"H")
saveCursor    :: String; saveCursor    = (esc:csi:"s")
restoreCursor :: String; restoreCursor = (esc:csi:"u")
getCursor     :: String; getCursor     = (esc:csi:"6n")
midScreen     :: String; midScreen     = (esc:csi:"12;1H")

setCursor :: Point -> String
setCursor (Point x y) = (esc:csi:show (y+1))++(';':show ((x+1)*2))++"H"

line :: Int -> String
line l = (esc:csi:show l)++(";1H")

data Mode = AutonomM | MasterM String | SlaveM String
  deriving (Show, Read)

type Input = (Int, Handle)

data Element = Empty | Wall | Player | Slave | Bomb | Blast
  deriving (Show, Read, Eq)
type Field  = [[Element]]
type Canvas = Field

type Bombs  = [Point]
type Blasts = [Bombs]

data GameState = GameState {move     :: TVar (Maybe Move),
                            repaint  :: TVar Bool,
                            field    :: TVar Field,
                            lifes    :: TVar Int,
                            player   :: TVar Point,
                            slaves   :: TVar [TVar Point],
                            repaints :: TVar [TVar Bool],
                            plBombs  :: TVar Bombs,
                            plBlasts :: TVar Blasts,
                            plBCount :: TVar Int,
                            bombs    :: TVar [TVar Bombs],
                            blasts   :: TVar [TVar Blasts],
                            bCounts  :: TVar [TVar Int],
                            quit     :: TVar Bool,
                            quits    :: TVar [TVar Bool]}
  deriving (Show, Read)

data Move = MoveLeft | MoveRight | MoveUp | MoveDown
          | DropBomb | Dead
  deriving (Eq, Show, Read)

data View = View Field Point [Point] Bombs Blasts
  deriving Show

instance Dist Move where
  finTVars _   = return ()
  regTVars _ _ = return ()

instance Dist Point where
  finTVars _   = return ()
  regTVars _ _ = return ()

instance Dist Element where
  finTVars _   = return ()
  regTVars _ _ = return ()

gKeyCount :: MVar (Int, Int)
gKeyCount  = unsafePerformIO (newMVar (0,0))

main :: IO ()
main = CE.catch (do
  debugStrLn6 "### bomberman main"
  putStr clearScreen
  hSetEcho stdin False
  hSetBuffering stdin NoBuffering
  args <- getArgs
  debugStrLn6 (show args)
  (mode, input) <- gameMode args
  debugStrLn6 (show mode)
  debugStrLn6 (show input)
  startDist (startGame mode input benchmark)
  --showTCPStat (midScreen++"quit")
  )(\(e::SomeException) -> error ("error in Bomberman: " ++ show e))

gameMode :: [String] -> IO (Mode, Input)
gameMode ((mode:_):[])      | elem mode aKeys = -- autonom + interactive
  return (AutonomM, (1, stdin))
gameMode ((mode:_):input:[]) | elem mode aKeys = -- autonom + file
  (openFile input ReadMode) >>= \h -> return (AutonomM, (1, h))
gameMode ((mode:_):[])      | elem mode mKeys = -- master + interactive
  return (MasterM gDefaultNameServer, (1, stdin))
gameMode ((mode:_):input:[]) | elem mode mKeys = -- master + file
  (openFile input ReadMode) >>= \h -> return (MasterM gDefaultNameServer, (1,h))
gameMode ((mode:_):delay:input:[]) | elem mode mKeys = do -- master+delay+file
  h <- (openFile input ReadMode)
  return (MasterM gDefaultNameServer, (read delay, h))
gameMode ((mode:_):[])                 | elem mode sKeys = -- slave+interactive
  return (SlaveM gDefaultNameServer, (1, stdin))
gameMode ((mode:_):input:[])           | elem mode sKeys = -- slave + file
  (openFile input ReadMode) >>= \h -> return (SlaveM gDefaultNameServer, (1, h))
gameMode ((mode:_):delay:input:[])  | elem mode sKeys = do -- slave+delay+file
  h <- (openFile input ReadMode)
  return (SlaveM gDefaultNameServer, (read delay, h))
gameMode ((mode:_):input:nameServer:[]) | elem mode sKeys = -- s+f+nameserver
  (openFile input ReadMode) >>= \h -> return (SlaveM nameServer, (1, h))
gameMode ((mode:_):delay:input:nameServer:[]) | elem mode sKeys = do --s+d+f+n
  h <- (openFile input ReadMode)
  return (SlaveM nameServer, (read delay, h))
gameMode _ = getInitKey >>= \mode -> return (mode, (1, stdin)) -- interactive

benchmark :: IO Int -> Mode -> Input -> IO ()
benchmark action mode (delayStep, file) = do
  debugStrLn6 "### bomberman benchmark1"
  debugStrLn1 "### bomberman benchmark1"
  start     <- getProcessTimes
  debugStrLn1 "### bomberman benchmark2"
  lostLifes <- action
  debugStrLn1 "### bomberman benchmark3"
  end       <- getProcessTimes
  debugStrLn1 "### bomberman benchmark4"
  debugStrLn6 "### bomberman benchmark2"
  clockTick <- getSysVar ClockTick
  hostname  <- getHostName
  (keysPressed, keysProcessed)  <- readMVar gKeyCount
  let elapsed = toInteger (fromEnum ((elapsedTime end) - (elapsedTime start)))
      user    = toInteger (fromEnum ((userTime end) - (userTime start)))
      system  = toInteger (fromEnum ((systemTime end) - (systemTime start)))
      delay   = toInteger delayStep * clockTick * toInteger keysPressed `div` 20
      filler  = user + system + delay - elapsed
  putStrLn ("\nHost: " ++ hostname)
  putStrLn ("Input file: " ++ show file ++ "\n")
  putStr (" elapsed: " ++ showJustify elapsed 5 ++ " ticks   ")
  putStrLn ("->   elapsed time total: "++ ticks2Sec elapsed clockTick)
  putStr ("=   user: " ++ showJustify user 5 ++ " ticks   ")
  putStrLn ("     (" ++ show clockTick ++ " ticks/sec)")
  putStr ("+ system: " ++ showJustify system 5 ++ " ticks   ")
  putStrLn ("->   user + system time: "++ ticks2Sec (user + system) clockTick)
  putStr ("+  delay: " ++ showJustify delay 5 ++ " ticks   ")
  putStrLn ("<-   "++show keysPressed ++ "/" ++ show keysProcessed ++ " keys read/processed")
  putStr ("- filler: " ++ showJustify filler 5 ++ " ticks   ")
  putStrLn ("     ("++show (delayStep*50)++" ms/key stroke)")
  putStr (showLifes lostLifes)
  putStrLn ("               Mode: " ++ show mode ++ "   ")

ticks2Sec :: Integer -> Integer -> String
ticks2Sec t ticksPerSec =
  let (sec, ticks) = divMod t ticksPerSec
      ms = ticks * 1000 `div` ticksPerSec
  in (show sec ++ "." ++ (concat [show ((ms `mod` (x * 10)) `div` x) | x <- [100,10,1]]) ++ " s")

showLifes :: Int -> String
showLifes l | l == 1 = "  1 life lost "
            | otherwise = showJustify (toInteger l) 3 ++ " lifes lost"

showJustify :: Show a => a -> Int -> String
showJustify i len = let s = show i in (replicate (len - length s) ' ') ++ s

launchGame :: Input -> GameState -> IO Int
launchGame input gameState = do
  debugStrLn6 ("### bomberman launchGame")
  showGameState gameState
  forkIO (view gameState) >> return ()
  forkIO (playerCtrl gameState) >> return ()
  debugStrLn1 ("### bomberman launchGame 0")
  processKeyboard gameState input
  debugStrLn1 ("### bomberman launchGame 1")
  atomic $ readTVar (lifes gameState)

startGame :: Mode -> Input -> (IO Int -> Mode -> Input -> IO ()) -> IO ()
startGame mode input bench = do
  debugStrLn6 "### bomberman startGame"
  debugStrLn6 (show mode)
  debugStrLn6 (show input)
  case mode of
    AutonomM       -> bench (initAuto >>= launchGame input) mode input
    MasterM server -> do
                      gameState <- initMaster server
                      threadDelay cDelay
                      bench (launchGame input gameState) mode input
                      termMaster server
                      threadDelay cDelay
    SlaveM server  -> do
                      gameState <- initSlave server
                      threadDelay cDelay
                      bench (launchGame input gameState) mode input
                      threadDelay cDelay

initMaster :: String -> IO GameState
initMaster server = do
  debugStrLn6 ("### bomberman initMaster")
  game <- initAuto
  atomic $ do
    writeTVar (slaves game) [player game]
  registerTVar server (quits game) "QUITS"
  registerTVar server (field game) "FIELD"
  registerTVar server (slaves game) "SLAVES"
  registerTVar server (repaints game) "REPAINTS"
  registerTVar server (bombs game) "BOMBS"
  registerTVar server (blasts game) "BLASTS"
  registerTVar server (bCounts game) "BCOUNTS"
  return game

termMaster :: String -> IO ()
termMaster server = do
  debugStrLn6 ("### bomberman terminateMaster")
  deregisterTVar server "QUITS"
  deregisterTVar server "FIELD"
  deregisterTVar server "SLAVES"
  deregisterTVar server "REPAINTS"
  deregisterTVar server "BOMBS"
  deregisterTVar server "BLASTS"
  deregisterTVar server "BCOUNTS"

initSlave :: String -> IO GameState
initSlave server = CE.catch (do
  debugStrLn6 ("### bomberman initSlave")
  mQuits <- lookupTVar server "QUITS"
  let sQuits = fromJust mQuits
  mField <- lookupTVar server "FIELD"
  let sField = fromJust mField
  mSlaves <- lookupTVar server "SLAVES"
  let sSlaves = fromJust mSlaves
  mRepaints <- lookupTVar server "REPAINTS"
  let sRepaints = fromJust mRepaints
  mBombs <- lookupTVar server "BOMBS"
  let sBombs = fromJust mBombs
  mBlasts <- lookupTVar server "BLASTS"
  let sBlasts = fromJust mBlasts
  mBCounts <- lookupTVar server "BCOUNTS"
  let sBCounts = fromJust mBCounts
  myLives   <- atomic $ newTVar cInitLifes
  newSlave  <- atomic $ newTVar cInitPlayer
  sMove     <- atomic $ newTVar Nothing
  myBombs   <- atomic $ newTVar []
  myBlasts  <- atomic $ newTVar []
  myBCount  <- atomic $ newTVar 0
  myRepaint <- atomic $ newTVar True
  myQuit    <- atomic $ newTVar False
  atomic $ do
    oldSlaves <- readTVar sSlaves
    writeTVar sSlaves (newSlave:oldSlaves)
    oldBombs <- readTVar sBombs
    writeTVar sBombs (myBombs:oldBombs)
    oldBlasts <- readTVar sBlasts
    writeTVar sBlasts (myBlasts:oldBlasts)
    oldBCounts <- readTVar sBCounts
    writeTVar sBCounts (myBCount:oldBCounts)
    otherRepaints <- readTVar sRepaints
    writeTVar sRepaints (myRepaint:otherRepaints)
    otherQuits <- readTVar sQuits
    writeTVar sQuits (myQuit:otherQuits)
  return (GameState sMove myRepaint sField myLives newSlave sSlaves sRepaints myBombs myBlasts myBCount sBombs sBlasts sBCounts myQuit sQuits)
   )(\(e::SomeException) -> putStrLn (midScreen ++ show e ++
                    " in initSlave from "++ show server ++
                    ". Starting auto game.") >> initAuto)

initAuto :: IO GameState
initAuto = do
  debugStrLn6 ("### bomberman initAuto")
  initField <- readField
  atomic $ do
    aMove     <- newTVar Nothing
    aRepaint  <- newTVar True
    aField    <- newTVar initField
    aLives    <- newTVar cInitLifes
    aPlayer   <- newTVar cInitPlayer
    aSlaves   <- newTVar []
    aRepaints <- newTVar [aRepaint]
    aPlBombs  <- newTVar []
    aPlBlasts <- newTVar []
    aPLBCount <- newTVar 0
    aBombs    <- newTVar [aPlBombs]
    aBlasts   <- newTVar [aPlBlasts]
    aBCounts  <- newTVar [aPLBCount]
    aQuit     <- newTVar False
    aQuits    <- newTVar [aQuit]
    return (GameState aMove aRepaint aField aLives aPlayer aSlaves aRepaints aPlBombs aPlBlasts aPLBCount aBombs aBlasts aBCounts aQuit aQuits)

readField :: IO Field
readField = do
  fieldStr <- readFile fieldFile
  return (map (map readEl) (lines fieldStr))

readEl :: Char -> Element
readEl c = if (c == cWallSymbol) then Wall else Empty

view :: GameState -> IO ()
view game = CE.catch (do
  debugStrLn6 ("### bomberman view")
  initField <- readField -- nur test
  debugStrLn6 ("### bomberman view 1")
  newView <- atomic $ do
    b <- readTVar (repaint game)
    proc $ debugStrLn5 ("view 0")
    if b
      then do
        writeTVar (repaint game) False

        f1 <- readTVar (field game)
        writeTVar (field game) initField
        writeTVar (field game) initField
        writeTVar (field game) f1
--
        p1 <- readTVar (player game)
        writeTVar (player game) cInitPlayer
        p2 <- readTVar (player game)
        writeTVar (player game) cInitPlayer
        writeTVar (player game) p1
 --}
        proc $ debugStrLn6 ("view 0")
        f <- readTVar (field game)
        proc $ debugStrLn6 ("view 1")
        p <- readTVar (player game)
        proc $ debugStrLn6 ("view 2")
        bsTVars <- readTVar (bombs game)
        allBombs <- mapM readTVar bsTVars
        proc $ debugStrLn6 ("view 3")
        blsTVars <- readTVar (blasts game)
        allBlasts <- mapM readTVar blsTVars
        proc $ debugStrLn6 ("view 4")
        slavesTVars <- readTVar (slaves game)
        proc $ debugStrLn6 ("view 5")
        slavesPts <- mapM readTVar slavesTVars
        proc $ debugStrLn6 ("view 6")
        return (View f p slavesPts (concat allBombs) (concat allBlasts))
      else retry
  debugStrLn6 ("### bomberman view 2")
  draw (makeCanvas newView)
  debugStrLn6 ("### bomberman view 3")
  printStatistics (midScreen)
  debugStrLn6 ("### bomberman view 4")
  view game
  )(\(e::SomeDistTVarException) -> do
    debugStrLn1 ("view catch "++show e)
    atomic $ do
      proc $ debugStrLn1 ("view catch atomic")
      cleanupSTM e game
    debugStrLn1 ("view catch dyn: <- ")
    view game
   )

cleanupSTM :: SomeDistTVarException -> GameState -> STM ()
cleanupSTM err game = do
  --proc $ debugStrLn1 ("--> cleanupSTM bsTVars")
  bsTVars <- readTVar (bombs game)
  let new =  (removeErrTVars err bsTVars)
  --proc $ debugStrLn1 ("--> bsTVars "++ show bsTVars ++ show new)
  writeTVar (bombs game) (removeErrTVars err bsTVars)
  blsTVars <- readTVar (blasts game)
  --proc $ debugStrLn1 ("--> blsTVars "++ show blsTVars)
  writeTVar (blasts game) (removeErrTVars err blsTVars)
  bCtTVars <- readTVar (bCounts game)
  --proc $ debugStrLn1 ("--> bCtTVars "++ show bCtTVars)
  writeTVar (bCounts game) (removeErrTVars err bCtTVars)
  slavesTVars <- readTVar (slaves game)
  --proc $ debugStrLn1 ("--> slavesTVars "++ show slavesTVars)
  writeTVar (slaves game) (removeErrTVars err slavesTVars)
  repaintsTVars <- readTVar (repaints game)
  --proc $ debugStrLn1 ("--> repaintsTVars "++ show repaintsTVars)
  writeTVar (repaints game) (removeErrTVars err repaintsTVars)
  --proc $ debugStrLn1 ("<-- cleanupSTM bsTVars")

removeErrTVars :: SomeDistTVarException -> [TVar a] -> [TVar a]
removeErrTVars ex tVars = [tVar | tVar <- tVars, not (isDistErrTVar ex tVar)]

draw :: Canvas -> IO ()
draw canvas = do
  putStr saveCursor
  putStr home
  mapM_ (putStrLn.concatMap elToStr) canvas
  putStr restoreCursor

elToStr :: Element -> String
elToStr e = case e of
  Empty  -> emptyImage
  Wall   -> wallImage
  Player -> playerImage
  Slave  -> slaveImage
  Bomb   -> bombImage
  Blast  -> blastImage

processKeyboard :: GameState -> Input -> IO ()
processKeyboard game input@(delayStep, file) = CE.catch (do
  debugStrLn6 ("### bomberman processKeyboard")
  end <- processEnd game
  if end
    then hClose file
    else do
         key <- if delayStep /= 0
           then do -- pause between key strokes in micros
                threadDelay (delayStep*50000)
                oldMove <- atomic (readTVar (move game))
                let keyOk = if oldMove == Nothing then 1 else 0
                modifyMVar_ gKeyCount (\(key, ok) -> return (key + 1, ok+keyOk))
                getGameKey file
           else do
                atomic $ do
                       oldMove <- readTVar (move game)
                       if oldMove /= Nothing then retry else return ()
                modifyMVar_ gKeyCount (\(key, ok) -> return (key + 1, ok + 1))
                getGameKey file
         debugStrLn6 ("### bomberman Key= "++show key)
         case key of
           QKey     -> atomic (writeTVar (quit game) True)
                       >> debugStrLn1 ("### bomberman Key= "++show key)
                       >> processKeyboard game input
           LeftKey  -> atomic (writeTVar (move game) (Just MoveLeft))
                       >> processKeyboard game input
           RightKey -> atomic (writeTVar (move game) (Just MoveRight))
                       >> processKeyboard game input
           UpKey    -> atomic (writeTVar (move game) (Just MoveUp))
                       >> processKeyboard game input
           DownKey  -> atomic (writeTVar (move game) (Just MoveDown))
                       >> processKeyboard game input
           SpaceKey -> atomic (writeTVar (move game) (Just DropBomb))
                       >> processKeyboard game input
           DebugKey -> startGDebug >> processKeyboard game input
           _        -> processKeyboard game input
  )(\(e::SomeDistTVarException) -> do
    debugStrLn1 ("processKeyboard catch "++show e)
    hClose file
   )

processEnd :: GameState -> IO Bool
processEnd game = CE.catch (do
  debugStrLn6 ("### bomberman processEnd")
  atomic $ do  -- eof + 0 bombs + all nodes done -> end w/o busy waiting
    eof <- readTVar (quit game)
    if eof
      then retry
      else return False
    `orElse` do
      --proc $ debugStrLn1 ("processEnd 1 ")
      count <- getSTMBombCount game -- # of active bombs
      --proc $ debugStrLn1 ("processEnd 2 ")
      qTVars <- readTVar (quits game)  -- list of quit status of all nodes
      proc $ debugStrLn1 ("processEnd 3 "++ show count)
      qs <- mapM readTVar qTVars
      proc $ debugStrLn1 ("processEnd 4 " ++ show qs)
      if (count == 0) && (and qs)
        then return True
        else retry
  )(\(e::SomeDistTVarException) -> do
    debugStrLn1 ("processEnd catch "++show e)
    atomic $ do
      qTVars <- readTVar (quits game)
      writeTVar (quits game) (removeErrTVars e qTVars)
      cTVars <- readTVar (bCounts game)
      writeTVar (bCounts game) (removeErrTVars e cTVars)
    processEnd game
   )

getSTMBombCount :: GameState -> STM Int
getSTMBombCount game = do -- # of active bombs
  countTVars <- readTVar (bCounts game)
  counts <- mapM readTVar countTVars
  return (foldr (+) 0 counts)

getGameKey :: Handle -> IO Key
getGameKey input = do
  c <- hGetChar input
  if (elem c ctrlKeys)
    then if (c == esc)
           then do
                c1 <- hGetChar input
                if (c1 == csi)
                  then do
                       c2 <- hGetChar input
                       if (elem c2 cursKeys)
                         then return (cursorKey c2)
                         else getGameKey input
                  else getGameKey input
           else return (controlKey c)
    else getGameKey input

cursorKey :: Char -> Key
cursorKey c =
  if (c == upKey) then UpKey
    else if (c == downKey) then DownKey
           else if (c == leftKey) then LeftKey
                  else if (c == rightKey) then RightKey
                         else error ("error wrong char '"++(c:"' in cursorKey"))

controlKey :: Char -> Key
controlKey c =
  if (elem c qKeys) then QKey
    else if (c == spaceKey) then SpaceKey
           else if (elem c dKeys) then DebugKey
                  else error ("error wrong char '"++(c:"' in controlKey"))

getInitKey :: IO Mode
getInitKey = do
  putStr clearScreen
  putStr (line 15)
  putStrLn"Bitte waehlen: Autonom (A), Master (M), Slave (S)"
  hSetEcho stdin False
  hSetBuffering stdin NoBuffering
  c <- getChar
  if (elem c initKeys)
    then initMode c
    else return AutonomM

initMode :: Char -> IO Mode
initMode c =
  if (elem c aKeys) then return AutonomM
   else if (elem c mKeys) then do
                               server <- getNameServer
                               return (MasterM server)
       else if (elem c sKeys) then do
                                   server <- getNameServer
                                   return (SlaveM server)
              else error ("error wrong char '"++(c:"' in initMode"))

getNameServer :: IO String
getNameServer = do
  hSetEcho stdin True
  putStrLn "Masterserver [default: localhost]: "
  s <- getLine
  hSetEcho stdin False
  return (if (s == []) then gDefaultNameServer else s)

playerCtrl :: GameState -> IO ()
playerCtrl game = CE.catch (do
  debugStrLn6 ("### bomberman playerCtrl")
  -- get new move if available orElse check if dead on explosion
  m <- atomic $   getSTMCommand (move game) -- get move or retry
         `orElse` do -- check active explosions
                  expls <- getSTMBombCount game -- # of active bombs
                  if expls > 0
                    then do
                         p <- readTVar (player game)
                         blsTVars <- readTVar (blasts game)
                         allBlasts <- mapM readTVar blsTVars
                         -- check player explosion
                         if elem p ((concat.concat) allBlasts)
                           then return Dead -- player exploded
                           else retry -- wait for next move
                    else retry -- wait for next move
  debugStrLn6 ("# case m "++show m)
  case m of -- make players move
    DropBomb -> forkIO (dropBomb game) >> return ()
    Dead     -> atomic $ do
                  p <- readTVar (player game)
                  blsTVars <- readTVar (blasts game)
                  allBlasts <- mapM readTVar blsTVars
                  if elem p ((concat.concat) allBlasts)
                    then retry
                    else modifyTVar (lifes game) (+1)
    _        -> atomic $ do
                  f <- readTVar (field game)
                  p <- readTVar (player game)
                  let newPlayer = case m of
                                    MoveLeft  -> legalPos f p pL
                                    MoveRight -> legalPos f p pR
                                    MoveUp    -> legalPos f p pU
                                    MoveDown  -> legalPos f p pD
                                    _         -> legalPos f p pI
                  writeTVar (player game) newPlayer
                  setSTMRepaintAll (repaints game)
  debugStrLn6 ("# end loop")
  playerCtrl game
  )(\(e::SomeDistTVarException) -> do
    debugStrLn1 ("playerCtrl catch "++show e)
    atomic $ do
      proc $ debugStrLn1 ("playerCtrl catch atomic: ")
      cleanupSTM e game
    debugStrLn1 ("playerCtrl catch dyn ---: ")
    --bs <- atomic $ showSTMRepaintAll (repaints game)
    --debugStrLn1 ("reps " ++ show bs)
    atomic $ setSTMRepaintAll (repaints game)
    debugStrLn1 ("playerCtrl catch dyn <-: ")
    playerCtrl game
   )

getSTMCommand :: TVar (Maybe Move) -> STM Move
getSTMCommand mTVar = do
    m <- readTVar mTVar
    proc $ debugStrLn2 ("getSTMCommand: "++(show m))
    if (m == Nothing)
      then retry
      else do
        writeTVar mTVar Nothing
        return (fromJust m)

legalPos :: Field -> Point -> Point -> Point
legalPos f p moveP = if (isLegal newPos f) then newPos else p
  where newPos = p + moveP

inBoundsPos :: Point -> Point -> Point
inBoundsPos p moveP = if (isInBounds newPos) then newPos else p
  where newPos = p + moveP

isLegal :: Point -> Field -> Bool
isLegal p f = isInBounds p &&  ((f `at` p) /= Wall)

isInBounds :: Point -> Bool
isInBounds (Point x y) = x >= 0 && x < cFSize && y >= 0 && y < cFSize

dropBomb :: GameState -> IO ()
dropBomb game = do
  debugStrLn6 ("### bomberman dropBomb")
  debugStrLn6 "# add new bomb@currPos to list"
  pos <- atomic $ do -- add new bomb@currPos to player bombs list
    p <- readTVar (player game)
    modifyTVar (plBombs game) (p:)
    return p
  debugStrLn6 "# set repaint"
  saveSetRepaintAll game -- mark changes to show
  timeBomb game pos

timeBomb :: GameState -> Point -> IO ()
timeBomb game pos = CE.catch (do -- catch bombs from dropped players
  debugStrLn6 "# init sema wake"
  wake <- atomic $ newTVar False
  debugStrLn6 "# start sleep counter wake"
  forkIO (threadDelay cDelay >> atomic (writeTVar wake True)) -- start timer
  debugStrLn6 "# wait for wake counter orelse active explosions"
  atomic $ do
      w <- readTVar wake
      if w
        then return () -- timeout
        else retry -- check active explosion @ pos -> new bomb explodes also
   `orElse` do
      expls <- getSTMBombCount game -- # of active bombs
      if expls > 0
        then do -- sync with other expls
             blsTVars <- readTVar (blasts game)
             allBlasts <- mapM readTVar blsTVars
             if elem pos ((concat.concat) allBlasts)
               then return () -- dont wait for timeout
               else retry -- wait for timeout
        else retry -- wait for timeout
  debugStrLn6 "# end bomb"
  blastBomb game pos
  )(\(e::SomeDistTVarException) -> do -- clean up and dont wait for timeout
    debugStrLn1 ("timeBomb catch "++show e)
    atomic $ do
      proc $ debugStrLn1 ("timeBomb catch atomic: ")
      cleanupSTM e game
    debugStrLn1 ("timeBomb catch dyn ---: ")
    blastBomb game pos
   )

blastBomb :: GameState -> Point -> IO ()
blastBomb game pos = do
  debugStrLn6 ("### bomberman blastBomb")
  debugStrLn6 "# remove bomb@pos, add new blasts, clear field@blast, inc #blast"
  newBls <- atomic $ do
    modifyTVar (plBombs game) (filter (/=pos)) --remove new bomb from bombs list
    let newBls = neighbours pos
    modifyTVar (plBlasts game) (newBls:) -- add new blasts to blasts lists
    modifyTVar (field game) (updateField Empty newBls) -- remove bombed walls
    modifyTVar (plBCount game) (+1)
    return newBls
  saveSetRepaintAll game -- mark changes to show
  debugStrLn6 "# delay"
  threadDelay cDelay
  debugStrLn6 "# remove new blast, dec #blast"
  atomic $ do
    modifyTVar (plBlasts game) (filter (/=newBls)) -- remove new blast
    modifyTVar (plBCount game) (+ (-1))
  saveSetRepaintAll game -- mark changes to show
  debugStrLn6 "# end bomb"

makeCanvas :: View -> Canvas
makeCanvas v =
  [ [canvasEl v (Point x y) | x <- [0..cFSize-1] ] | y <- [0..cFSize-1] ]

canvasEl :: View -> Point -> Element
canvasEl (View vField vPlayer vSlaves vBombs vBlasts) p =
  if elem p (concat vBlasts)
    then Blast
    else if (vPlayer == p)
           then Player
           else if elem p vSlaves
                  then Slave
                  else if elem p vBombs
                         then Bomb
                         else vField `at` p

updateField :: Element -> [Point] -> Field -> Field
updateField el ps fd =
  [ [ f x y | x <- [0..cFSize-1] ] | y <- [0..cFSize-1] ]
    where f x y = if (elem (Point x y) ps)
                    then el else fd `at` (Point x y)
{-  [bVec | y <- [0..cFSize-1],
           let bVec = [bEl | x <- [0..cFSize-1],
                             let bEl = if (elem (Point x y) ps)
                                         then el
                                         else fd `at` (Point x y)]]
-}

neighbours :: Point -> [Point]
neighbours p = p:map (inBoundsPos p) [pL,pR,pU,pD]

at :: Field -> Point -> Element
canvas `at` (Point x y) = (canvas !! y) !! x

modifyTVar :: Dist a => TVar a -> (a -> a) -> STM ()
modifyTVar t f = do
  v <- readTVar t
  writeTVar t (f v)

setSTMRepaintAll :: TVar [TVar Bool] -> STM ()
setSTMRepaintAll allRepaints = do
  repaintsTVars <- readTVar allRepaints
  mapM_ (flip writeTVar True) repaintsTVars

saveSetRepaintAll :: GameState -> IO ()
saveSetRepaintAll game = CE.catch (atomic $ do
  repaintsTVars <- readTVar (repaints game)
  mapM_ (flip writeTVar True) repaintsTVars
  )(\(e::SomeDistTVarException) -> do
    debugStrLn1 ("saveSetRepaintAll catch "++show e)
    atomic $ do
      proc $ debugStrLn1 ("saveSetRepaintAll catch atomic")
      cleanupSTM e game
    debugStrLn1 ("saveSetRepaintAll catch dyn: --- ")
    saveSetRepaintAll game
    debugStrLn1 ("saveSetRepaintAll catch dyn: <- ")
   )

showSTMRepaintAll :: TVar [TVar Bool] -> STM [Bool]
showSTMRepaintAll allRepaints = do
  proc $ debugStrLn6 ("showSTMRepaintAll")
  repaintsTVars <- readTVar allRepaints
  proc $ debugStrLn6 ("showSTMRepaintAll "++show repaintsTVars)
  bs <- mapM readTVar repaintsTVars
  proc $ debugStrLn6 ("showSTMRepaintAll end "++show bs)
  return bs

showGameState :: GameState -> IO ()
showGameState g = atomic $ do
  mv <- readTVar (move g)
  proc $ debugStrLn6 ("TVar move= "++show (move g)++" move= "++show mv)
  rp <- readTVar (repaint g)
  proc $ debugStrLn6 ("TVar repaint= "++show (repaint g)++" repaint= "++show rp)
  fd <- readTVar (field g)
  proc $ debugStrLn6 ("TVar field= "++show (field g)++" field= "++show fd)
  pl <- readTVar (player g)
  proc $ debugStrLn6 ("TVar player= "++show (player g)++" player= "++show pl)
  sl <- readTVar (slaves g)
  proc $ debugStrLn6 ("TVar slaves= "++show (slaves g)++" slaves= "++show sl)
  rs <- readTVar (repaints g)
  proc $ debugStrLn6 ("TVar repaints= "++show (repaints g)++" repaints= "++show rs)
  bs <- readTVar (bombs g)
  proc $ debugStrLn6 ("TVar bombs= "++show (bombs g)++" bombs= "++show bs)
  bl <- readTVar (blasts g)
  proc $ debugStrLn6 ("TVar blasts= "++show (blasts g)++" blasts= "++show bl)
  bc <- readTVar (bCounts g)
  proc $ debugStrLn6 ("TVar bCounts= "++show (bCounts g)++" bCount= "++show bc)
