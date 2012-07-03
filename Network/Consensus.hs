module Network.Consensus
  (listen)
where

import Network (PortID (..), withSocketsDo, listenOn, accept)
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (newTVar, atomically)
import Data.ByteString.Char8 (pack, unpack)
import System.IO (hPutStrLn, hGetLine, hFlush)

import qualified Data.Map as M
import qualified Network.Consensus.Protocol as C

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

command :: C.Repository -> [String] -> IO String
command entries ["PREPARE", k, r] =
  C.prepare entries k (read r) >>= 
  return . show

command entries ["ACCEPT", k, r, c] =
  C.accept entries k (read r) (pack c) >>=
  return . show

command entries ["LISTEN", k] =
  C.listen entries k (\k _ _ -> putStrLn ("comitted " ++ k)) >>
  return "OK"

command entries ["GET", k] =
  C.get entries k >>= \e ->
  return $ maybe "NOT FOUND" unpack e

command _ cmd = return $ "Unknown command " ++ (unwords cmd)
