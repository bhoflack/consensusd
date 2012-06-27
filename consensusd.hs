module Main (main) where

import Network (PortID (..), withSocketsDo, listenOn, accept)
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (TVar(..), atomically, newTVar, readTVar, modifyTVar)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack)
import System.Environment (getArgs)
import System.IO (hPutStrLn, hGetLine, hFlush)

import qualified Data.Map as M

type Key = String
type Revision = Int
type Content = ByteString

data Entry = Prepare Revision
           | Accept Revision Content (Maybe Revision)
           deriving (Eq, Show)

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

command entries ["ACCEPT", k, r, c] =
  accept' entries k (read r) (pack c) >>=
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
                        then return $ Nack k p 
                        else do  
                            modifyTVar rep (M.insert k entry)
                            return $ Prepared k r
                          where entry = Prepare r
    Just (Accept ar ac Nothing) -> if ar >= r
                                   then return $ Nack k ar
                                   else do
                                       modifyTVar rep (M.insert k entry)
                                       return $ Prepared k r
                                      where entry = Accept ar ac (Just r)
    Just (Accept ar ac (Just ap)) -> if ap >= r
                              then return $ Nack k ap
                              else do
                                  modifyTVar rep (M.insert k entry)
                                  return $ Prepared k r
                                 where entry = Accept ar ac (Just r)

data AcceptResponse = AcceptNack Key Revision
                    | NoPromise Key
                    | Accepted Key Revision Content
                    deriving (Show, Eq)

accept' rep k r c = atomically $ do
  d <- readTVar rep
  case (M.lookup k d) of
    Nothing -> return $ NoPromise k
    Just (Prepare p) -> if p == r
                        then let entry = Accept r c Nothing in do
                             modifyTVar rep (M.insert k entry)
                             return $ Accepted k r c
                        else return $ AcceptNack k p
    Just (Accept ar _ Nothing) -> return $ AcceptNack k ar
    Just (Accept _ _ (Just ap)) -> if ap == r
                                     then let entry = Accept r c Nothing in do
                                           modifyTVar rep (M.insert k entry)
                                           return $ Accepted k r c
                                     else return $ AcceptNack k ap
