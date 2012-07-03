module Network.Consensus.Protocol
  (Key,
   Revision, 
   Content,
   Listener,
   Entry (..),
   Repository,
   PrepareResponse,
   prepare,
   accept,
   listen,
   get)
where

import Control.Concurrent.STM (TVar (..), STM, atomically, readTVar, writeTVar)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack, unpack)

import qualified Data.Map as M

type Key = String
type Revision = Int
type Content = ByteString
type Listener = Key -> Revision -> Content -> IO ()

data Entry = Prepare Revision [Listener]
           | Accept Revision Content (Maybe Revision) [Listener]
           | NoEntry [Listener]

type Repository = TVar (M.Map String Entry)

data PrepareResponse = Nack Key Revision
                     | Prepared Key Revision
                     deriving (Show, Eq)

modifyTVar :: TVar a -> (a -> a) -> STM ()
modifyTVar var f = do
  x <- readTVar var
  writeTVar var (f x)

prepare :: TVar (M.Map Key Entry)
    -> Key
    -> Revision
    -> IO PrepareResponse
prepare rep k r = atomically $ do
  d <- readTVar rep     
  case (M.lookup k d) of
    Nothing -> do
           modifyTVar rep (M.insert k entry) 
           return $ Prepared k r
         where entry = Prepare r []
    Just (Prepare p ls) -> if p >= r 
                        then return $ Nack k p 
                        else do  
                            modifyTVar rep (M.insert k entry)
                            return $ Prepared k r
                          where entry = Prepare r ls
    Just (Accept ar ac Nothing ls) -> if ar >= r
                                   then return $ Nack k ar
                                   else do
                                       modifyTVar rep (M.insert k entry)
                                       return $ Prepared k r
                                      where entry = Accept ar ac (Just r) ls
    Just (Accept ar ac (Just ap) ls) -> if ap >= r
                              then return $ Nack k ap
                              else do
                                  modifyTVar rep (M.insert k entry)
                                  return $ Prepared k r
                                 where entry = Accept ar ac (Just r) ls
    Just (NoEntry ls) -> do
        modifyTVar rep (M.insert k entry)
        return $ Prepared k r
      where entry = Prepare r ls

data AcceptResponse = AcceptNack Key Revision
                    | NoPromise Key
                    | Accepted Key Revision Content
                    deriving (Show, Eq)

accept :: TVar (M.Map Key Entry)
     -> Key
     -> Revision
     -> Content
     -> IO Network.Consensus.Protocol.AcceptResponse
accept rep k r c = do 
  d <- atomically $ readTVar rep
  case (M.lookup k d) of
    Nothing -> return $ NoPromise k
    Just (Prepare p ls) -> 
      if p == r
      then let entry = Accept r c Nothing ls in 
      do
        atomically $ modifyTVar rep (M.insert k entry)
	mapM_ (\f -> f k r c) ls
        return $ Accepted k r c
      else return $ AcceptNack k p
    Just (Accept ar _ Nothing ls) -> return $ AcceptNack k ar
    Just (Accept _ _ (Just ap) ls) -> 
      if ap == r
      then let entry = Accept r c Nothing ls in 
      do
        atomically $ modifyTVar rep (M.insert k entry)
        mapM_ (\f -> f k r c) ls
        return $ Accepted k r c
      else return $ AcceptNack k ap
    Just (NoEntry ls) -> return $ NoPromise k

listen :: Ord k =>
     TVar (M.Map k Entry) 
     -> k 
     -> Listener 
     -> IO ()
listen rep k cb = do
  d <- atomically $ readTVar rep
  atomically $ case (M.lookup k d) of
    Nothing -> 
        modifyTVar rep (M.insert k entry)
      where entry = NoEntry [cb]
    Just (NoEntry ls) ->
        modifyTVar rep (M.insert k entry)
      where entry = NoEntry (cb : ls)
    Just (Prepare p ls) ->
        modifyTVar rep (M.insert k entry)
      where entry = Prepare p (cb : ls)
    Just (Accept r c p ls) ->
        modifyTVar rep (M.insert k entry)
      where entry = Accept r c p (cb : ls)

get :: Ord k =>
     TVar (M.Map k Entry)
     -> k
     -> IO (Maybe Content)
get rep k = do
  d <- atomically $ readTVar rep
  case (M.lookup k d) of
    Just (Accept r c _ _) -> return $ Just c
    _ -> return $ Nothing
