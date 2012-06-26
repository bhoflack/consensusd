module Main (main) where

import Network (PortID (..), withSocketsDo, listenOn, accept)
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (TVar(..), atomically, newTVar, readTVar, modifyTVar) 
import System.Environment (getArgs)
import System.IO (hPutStrLn, hGetLine, hFlush)

import qualified Data.Map as M

type Key = String
type Revision = Int

data Entry = Prepare Revision

type Repository = TVar (M.Map String Entry)

main = do
  args <- getArgs
  case args of
    [port] -> listen $ read port
    _ -> usage

usage = putStrLn "Usage: consensusd <<port number>>"


listen port = withSocketsDo $ do
  entries <- atomically $ newTVar M.empty
  sock <- listenOn (PortNumber (fromIntegral port))
  acceptLoop entries sock

acceptLoop entries sock = do
  (h, _, _) <- accept sock
  forkIO $ handle entries h
  acceptLoop entries sock

handle entries h = do
    line <- hGetLine h
    response <- runCommand line
    hPutStrLn h response
    hFlush h
    handle entries h
  where runCommand = (command entries) . words

command :: Repository -> [String] -> IO String
command entries ["PREPARE", k, r] =
  prepare entries k (read r) >>= 
  return . show

command _ cmd = return $ "Unknown command " ++ (unwords cmd)

data PrepareResponse = Nack Key Revision
                     | Prepared Key Revision
                     deriving (Show, Eq)

prepare rep k r = atomically $ do
  d <- readTVar rep     
  case (M.lookup k d) of
    Nothing -> do
           modifyTVar rep (M.insert k entry) 
           return $ Prepared k r
         where entry = Prepare r
    Just (Prepare p) -> if p >= r 
                        then return $ Nack k r 
                        else do  
                            modifyTVar rep (M.insert k entry)
                            return $ Prepared k r
                          where entry = Prepare r

