module Main (main) where

import Network
import Control.Concurrent
import System.Environment (getArgs)
import System.IO

import qualified Data.Map as M

main = do
  args <- getArgs
  case args of
    [port] -> listen $ read port
    _ -> usage

usage = putStrLn "Usage: consensusd <<port number>>"


listen port = withSocketsDo $ do
  sock <- listenOn (PortNumber (fromIntegral port))
  acceptLoop sock

acceptLoop sock = do
  (h, _, _) <- accept sock
  forkIO $ handle h
  acceptLoop sock

handle h = do
    line <- hGetLine h
    hPutStrLn h (runCommand line)
    hFlush h
    handle h
  where runCommand = command . words

command :: [String] -> String
command ["PREPARE", k, r] =
    show $ prepare d k (read r) 
  where d = M.empty
command cmd = "Unknown command " ++ (unwords cmd)

type Key = String
type Revision = Int

data PrepareResponse = Nack Key Revision
                     | Prepared Key Revision
                     deriving (Show, Eq)

prepare d k r = 
   case (M.lookup k d) of
    _ -> Nack k r
