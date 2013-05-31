{-# LANGUAGE CPP, NamedFieldPuns, RecordWildCards, ScopedTypeVariables, BangPatterns #-}

#if MIN_VERSION_monad_control(0,3,0)
{-# LANGUAGE FlexibleContexts #-}
#endif

#if !MIN_VERSION_base(4,3,0)
{-# LANGUAGE RankNTypes #-}
#endif

-- |
-- Module:      Data.Pool
-- Copyright:   (c) 2011 MailRank, Inc.
-- License:     BSD3
-- Maintainer:  Bryan O'Sullivan <bos@serpentine.com>
-- Stability:   experimental
-- Portability: portable
--
-- A high-performance striped pooling abstraction for managing
-- flexibly-sized collections of resources such as database
-- connections.
--
-- \"Striped\" means that a single 'Pool' consists of several
-- sub-pools, each managed independently.  A stripe size of 1 is fine
-- for many applications, and probably what you should choose by
-- default.  Larger stripe sizes will lead to reduced contention in
-- high-performance multicore applications, at a trade-off of causing
-- the maximum number of simultaneous resources in use to grow.
module Data.Pool
    (
      Pool(idleTime, maxResources, numStripes)
    , LocalPool
    , createPool
    , createPool'
    , withResource
    , takeResource
    , destroyResource
    , putResource
    ) where

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO, killThread, myThreadId, threadDelay)
import Control.Concurrent.STM
import Control.Concurrent.STM.Stats (getSTMStats, trackNamedSTM)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.Selectable
import Control.Exception (SomeException, onException)
import Control.Monad (forM_, forever, liftM2, unless, when)

import qualified Data.Map as M
import Data.Hashable (hash)
import Data.List (partition)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Time.Format (formatTime)

import System.Locale (defaultTimeLocale)
import System.IO (hPutStrLn, stderr)
import System.Mem.Weak (addFinalizer)
import qualified Control.Exception as E
import qualified Data.Vector as V

#if MIN_VERSION_monad_control(0,3,0)
import Control.Monad.Base (liftBase)
#else
import Control.Monad.IO.Class (liftIO)
#define control controlIO
#define liftBase liftIO
#endif

-- | A single resource pool entry.
data Entry a = Entry {
      entry :: a
    , lastUse :: UTCTime
    -- ^ Time of last return.
    , uses :: Int
    }

-- | A single striped pool.
data LocalPool a = LocalPool {
      inUse :: TVar Int
    -- ^ Count of open entries (both idle and in use).
    , entries :: TVar [Entry a]
    -- ^ Idle entries.
    }

data Pool a = Pool {
      create :: IO a
    -- ^ Action for creating a new entry to add to the pool.
    , destroy :: a -> IO ()
    -- ^ Action for destroying an entry that is now done with.
    , numStripes :: Int
    -- ^ Stripe count.  The number of distinct sub-pools to maintain.
    -- The smallest acceptable value is 1.
    , idleTime :: NominalDiffTime
    -- ^ Amount of time for which an unused resource is kept alive.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before closing may be a little longer than
    -- requested, as the reaper thread wakes at 1-second intervals.
    , maxResources :: Int
    -- ^ Maximum number of resources to maintain per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
    , localPools :: V.Vector (LocalPool a)
    -- ^ Per-capability resource pools.
    , maxUses :: Int
    -- ^ Maximum number of times a resource can be reused
    }

instance Show (Pool a) where
    show Pool{..} = "Pool {numStripes = " ++ show numStripes ++ ", " ++
                    "idleTime = " ++ show idleTime ++ ", " ++
                    "maxResources = " ++ show maxResources ++ "}"

createPool :: IO a -> (a -> IO ()) -> Int -> NominalDiffTime -> Int -> IO (Pool a)
createPool create destroy numStripes idleTime maxResources =
  createPool' create destroy numStripes idleTime 0 0 maxResources

createPool'
    :: IO a
    -- ^ Action that creates a new resource.
    -> (a -> IO ())
    -- ^ Action that destroys an existing resource.
    -> Int
    -- ^ Stripe count.  The number of distinct sub-pools to maintain.
    -- The smallest acceptable value is 1.
    -> NominalDiffTime
    -- ^ Amount of time for which an unused resource is kept open.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before destroying a resource may be a little
    -- longer than requested, as the reaper thread wakes at 1-second
    -- intervals.
    -> Int
    -- ^ Amount of seconds to delay broadcasting stats.  A value of zero
    -- will turn off stats.
    -> Int
    -- ^ Max uses per connection before it is destroyed
    -> Int
    -- ^ Maximum number of resources to keep open per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
     -> IO (Pool a)
createPool' create destroy numStripes idleTime trackTime maxUses maxResources = do
  when (numStripes < 1) $
    modError "pool " $ "invalid stripe count " ++ show numStripes
  when (idleTime < 0.5) $
    modError "pool " $ "invalid idle time " ++ show idleTime
  when (maxResources < 1) $
    modError "pool " $ "invalid maximum resource count " ++ show maxResources
  when (trackTime < 0) $
    modError "pool" $ "invalid stats tracking frequency " ++ show trackTime
  localPools <- atomically . V.replicateM numStripes $
                liftM2 LocalPool (newTVar 0) (newTVar [])
  reaperId <- forkIO $ reaper destroy idleTime localPools
  let p = Pool {
            create
          , destroy
          , numStripes
          , idleTime
          , maxResources
          , localPools
          , maxUses
          }
  when (trackTime > 0) $ do
    trackerId <- forkIO $ tracker localPools trackTime
    addFinalizer p $ killThread trackerId

  addFinalizer p $ killThread reaperId
  return p

-- | Periodically broadcast resource and STM stats. Frequency in seconds.
tracker :: V.Vector (LocalPool a) -> Int -> IO ()
tracker pools frequency = do
    start <- getCurrentTime
    track start 0
  where
    track !start !lastRetries = do
        threadDelay (frequency * 1000000)
        retries <- statRetriesToCommits "takeResource"
        now <- getCurrentTime
        V.forM_ pools $ \LocalPool{..} -> do
            (inuse, idle) <- trackNamedSTM "tracker" $ do
                inuse' <- readTVar inUse
                entries' <- readTVar entries
                return (inuse', length entries')
            logger now $ "Data.Pool {kept-alive=" ++ (show inuse) ++ ", idle=" ++ (show idle) ++ "}"
        let diff = timeSince now start
        let currentRetries = retries - lastRetries
        logger now $  "Data.Pool {retries/sec=" ++ (show $ fromIntegral currentRetries / diff) ++ "}"
        track now retries -- strict because of logger

logger :: UTCTime -> String -> IO ()
logger tm s = do
    let d = formatTime defaultTimeLocale standardFormat $ tm
    hPutStrLn stderr $ "[" ++ d ++ "] " ++ s
  where
    standardFormat = "%Y/%m/%d %H:%M:%S"

timeSince :: UTCTime -> UTCTime -> Double
timeSince now start = realToFrac (now `diffUTCTime` start)

statRetriesToCommits :: String -> IO Int
statRetriesToCommits name = do
  smap <- getSTMStats
  case M.lookup name smap of
    Just (_, retries) -> return retries
    Nothing -> return 0

-- | Periodically go through all pools, closing any resources that
-- have been left idle for too long.
reaper :: (a -> IO ()) -> NominalDiffTime -> V.Vector (LocalPool a) -> IO ()
reaper destroy idleTime pools = forever $ do
  threadDelay (1 * 1000000)
  now <- getCurrentTime
  let isStale Entry{..} = now `diffUTCTime` lastUse > idleTime
  V.forM_ pools $ \LocalPool{..} -> do
    resources <- atomically $ do
      (stale,fresh) <- partition isStale <$> readTVar entries
      unless (null stale) $ do
        writeTVar entries fresh
        modifyTVar_ inUse (subtract (length stale))
      return (map entry stale)
    forM_ resources $ \resource -> do
      destroy resource `E.catch` \(e::SomeException) -> hPutStrLn stderr $ "destroy: " ++ (show e)

-- | Temporarily take a resource from a 'Pool', perform an action with
-- it, and return it to the pool afterwards.
--
-- * If the pool has an idle resource available, it is used
--   immediately.
--
-- * Otherwise, if the maximum number of resources has not yet been
--   reached, a new resource is created and used.
--
-- * If the maximum number of resources has been reached, this
--   function blocks until a resource becomes available.
--
-- If the action throws an exception of any type, the resource is
-- destroyed, and not returned to the pool.
--
-- It probably goes without saying that you should never manually
-- destroy a pooled resource, as doing so will almost certainly cause
-- a subsequent user (who expects the resource to be valid) to throw
-- an exception.
withResource ::Pool a -> Int -> (a -> IO b) -> IO (Either SomeException b)
withResource pool timeout act = do
  res <- E.try $ takeResource pool
  case res of
      Left (e :: E.SomeException) -> do
          hPutStrLn stderr $ "takeResource: " ++ (show e)
          return $ Left e
      Right (resource, uses', local) -> do
        q <- newEmptyMVar
        _ <- forkIO $ do
             ret <- E.try $ act resource
             putMVar q ret

        res <- select (Check q) (Just timeout)
        case res of
            NotReady -> (error "request timeout") `E.catch` (\(e :: SomeException) -> do
                                                                destroyResource pool local resource

                                                                return $ Left e)
            Ready r -> do
                ret' <- takeMVar r
                case ret' of
                    Left (e' :: E.SomeException) -> do
                        destroyResource pool local resource
                        hPutStrLn stderr $ "withResource: " ++ (show e')
                        return $ Left e'
                    Right ret' -> do
                        putResource local resource uses'
                        return $ Right ret'
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE withResource #-}
#endif

-- | Take a resource from the pool, following the same results as
-- 'withResource'. Note that this function should be used with caution, as
-- improper exception handling can lead to leaked resources.
--
-- This function returns both a resource and the @LocalPool@ it came from so
-- that it may either be destroyed (via 'destroyResource') or returned to the
-- pool (via 'putResource').
takeResource :: Pool a -> IO (a, Int, LocalPool a)
takeResource Pool{..} = do
  i <- liftBase $ ((`mod` numStripes) . hash) <$> myThreadId
  let pool@LocalPool{..} = localPools V.! i
  (getResource, uses') <- trackNamedSTM "takeResource" $ do
    ents <- readTVar entries
    case ents of
      (Entry{..}:es) -> do
          -- handle resources that haven't been reaped yet
          writeTVar entries es
          case maxUses > 0 && uses >= maxUses of
              True -> do
                  let recreate = (destroy entry `E.catch` \e -> E.throw (e :: SomeException)) >> create
                  return (recreate `onException` atomically (modifyTVar_ inUse (subtract 1)), 0)
              False -> do
                let reuse = return entry
                return (reuse, uses)
      [] -> do
        -- when no idle resources
        used <- readTVar inUse
        when (used == maxResources) retry
        writeTVar inUse $! used + 1
        return $ (create `onException` atomically (modifyTVar_ inUse (subtract 1)), 0)
  resource <- getResource
  return (resource, uses', pool)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE takeResource #-}
#endif

-- | Destroy a resource. Note that this will ignore any exceptions in the
-- destroy function.
destroyResource :: Pool a -> LocalPool a -> a -> IO ()
destroyResource Pool{..} LocalPool{..} resource = do
   atomically (modifyTVar_ inUse (subtract 1))
   destroy resource `E.catch` \(e::SomeException) -> hPutStrLn stderr $ "destroy: " ++ (show e)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE destroyResource #-}
#endif

-- | Return a resource to the given 'LocalPool'.
putResource :: LocalPool a -> a -> Int -> IO ()
putResource LocalPool{..} resource uses' = do
    now <- getCurrentTime
    atomically $ modifyTVar_ entries (Entry resource now (uses'+1) :)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE putResource #-}
#endif

modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ v f = readTVar v >>= \a -> writeTVar v $! f a

modError :: String -> String -> a
modError func msg =
    error $ "Data.Pool." ++ func ++ ": " ++ msg
