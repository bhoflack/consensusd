module Main (main) where

import Network.Consensus (listen)
import System.Environment (getArgs)

main = do
  args <- getArgs
  case args of
    [port] -> listen $ read port
    _ -> usage

usage = putStrLn "Usage: consensusd <<port number>>"
