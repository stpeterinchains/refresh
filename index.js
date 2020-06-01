'use strict';

/** @module stpeterinchains-refresh */

// Import dependencies
const
  { createHash } = require('crypto'),
  { URL }        = require('url');

const
  { SecretManagerServiceClient } = require('@google-cloud/secret-manager'),
  { Octokit }                    = require('@octokit/rest'),
  Twitter                        = require('twitter-lite');

// Get parameters from environment
const
  { GCP_PROJECT                             : gcpProject,
    GITHUB_SECRET                           : githubSecret,
    GITHUB_OWNER                            : githubOwner,
    GITHUB_REPO                             : githubRepository,
    GITHUB_GENERATED_CONTENT_PATH           : githubGeneratedContentPath,
    TWITTER_SECRET                          : twitterSecret,
    TWITTER_REGULAR_EVENTS_COLLECTION_ID    : twitterRegularEventsCollectionId,
    TWITTER_REGULAR_EVENTS_COLLECTION_COUNT :
            twitterRegularEventsCollectionCount,
    TWITTER_SPECIAL_EVENTS_COLLECTION_ID    : twitterSpecialEventsCollectionId,
    TWITTER_SPECIAL_EVENTS_COLLECTION_COUNT :
            twitterSpecialEventsCollectionCount,
    TWITTER_BULLETINS_COLLECTION_ID         : twitterBulletinsCollectionId,
    TWITTER_BULLETINS_COLLECTION_COUNT      :
            twitterBulletinsCollectionCount,
    TWEET_CONTINUATION                      : tweetContinuationLine,
  } = process.env;

// Define support functions
const

  /**
   * Transforms a raw regular/special event tweet into an event object.
   * Handles continuation tweets (first line "..."), which append their
   * subsequent lines as descriptive lines to the event object of the
   * previous tweet.
   *
   * @function eventsTransform
   * @param {string} tweetText - Event tweet raw text
   * @param {(object|null)} lastNonContinuationEvent - Event object of previous tweet
   * @return {(object|null)} - Event object, or null if invalid tweet
   */

  eventsTransform =
    (tweetText, lastNonContinuationEvent) => {

      const
        [ firstLine,
          ...subsequentLines ] = tweetText.split('\n');

      if (firstLine !== tweetContinuationLine) {

        const
          titleLine               = firstLine,
          [ timesLine,
            ...descriptiveLines ] = subsequentLines;

        const
          [ , title ] =
            titleLine.
                  replace(/\+\w+/g, '').
                  match(/^\s*(.*?)\s*$/),
              // strip color directives, trim leading/trailing ws
          [ , color ] =
            titleLine.match(/\+(\w+)/) ||
                  [ undefined, null ];
              // extract first available color directive

        const
          times        = [],
          timeSegments =
            timesLine ?
                  timesLine.split(';') :
                  [];

        for (const timeSegment of timeSegments) {

          const
            [ ,
              day,
              time,
              location = null, ] =
                    timeSegment.match(
                      /^\s*(.*?)\s*@\s*(.*?)(?:\s*\(\s*(.*?)\s*\).*?)?\s*$/) ||
                          [ undefined, null, null, null ];
                      // extract day, time, location

          if (day && time)
            times.push({ day, time, location });
        }

        const descriptive = [];

        for (const rawLine of descriptiveLines) {

          const
            [ , line ] = rawLine.match(/^\s*(.*?)\s*$/);
              // trim leading/trailing ws

          if (line)
            descriptive.push(line);
        }

        const
          event = { title, color, times, descriptive };

        if (title && times.length > 0)
          return [ event, event ];
        else
          return [ null, null ];
      }

      else {

        if (lastNonContinuationEvent) {

          const
            { descriptive }  = lastNonContinuationEvent,
            descriptiveLines = subsequentLines;

          for (const lineRaw of descriptiveLines) {

            const
              [ , line ] = lineRaw.match(/^\s*(.*?)\s*$/);
                // trim leading/trailing ws

            if (line)
              descriptive.push(line);
          }

          return [ null, lastNonContinuationEvent ];
        }

        else
          return [ null, null ];
      }
    },

  /**
   * Transforms a raw bulletin tweet into a bulletin object.
   *
   * @function bulletinsTransform
   * @param {stromg} tweetText - Bulletin tweet raw text
   * @return {(object|null)} - Bulletin object, or null if invalid tweet
   */

  bulletinsTransform =
    tweetText => {

      const
        [ bulletinDateTitleLine,
          bulletinLinkLine,
          ...subsequentLines ] = tweetText.split('\n');

      const
        [ ,
          date,
          title, ] =
                bulletinDateTitleLine.match(/^\s*(.*?)\s*-\s*(.*?)\s*$/) ||
                      [ undefined, null, null ],
                  // extract date and bulletin title, trim leading/trailing ws
        [ , link ] =
          bulletinLinkLine.match(/^\s*(.*?)\s*$/);  // trim leading/trailing ws

      if (! date || ! title)
        return [ null ];

      try { new URL(link); }
      catch (error) { return [ null ]; }

      const inserts = [];

      let
        insertTitle,
        insertLink,
        expectingInsertTitleLine = true;

      for (const rawLine of subsequentLines) {

        const
          [ , trimmedLine ] = rawLine.match(/^\s*(.*?)\s*$/);
            // trim leading/trailing ws

        if (trimmedLine) {

          if (expectingInsertTitleLine) {

            insertTitle              = trimmedLine;
            expectingInsertTitleLine = false;
          }

          else {

            insertLink               = trimmedLine;
            expectingInsertTitleLine = true;

            inserts.push(
              { title : insertTitle,
                link  : insertLink, });
          }
        }
      }

      const
        bulletin = { date, title, link, inserts };

      return [ bulletin ];
    };

// Define export function

/**
 * Google Cloud Function which reads tweets from each of three Twitter
 * collections containing tweets describing the regular events, special events,
 * and bulletins to be displayed by the accompanying website. The function
 * extracts the content of the tweets into objects and commits a single JSON
 * file containing the resulting datasets to Github to trigger website
 * regeneration.
 *
 * @async
 * @function agent
 * @param {object} event - GCP PubsubMessage object. Ignored.
 * @param {object} context - GCP Function context object. Ignored.
 * @see {@link https://cloud.google.com/functions/docs/writing/background}
 * @see {@link https://cloud.google.com/functions/docs/calling/pubsub}
 */

exports.agent =
  async () => {

    try {

      // Get parameters from Google Secret Manager
      const
        githubSecretResourceName =
          `projects/${gcpProject}/secrets/${githubSecret}/versions/latest`,
        twitterSecretResourceName =
          `projects/${gcpProject}/secrets/${twitterSecret}/versions/latest`;

      const
        gcpSecretManager =
          new SecretManagerServiceClient(),
        [ [ { payload : { data : githubCredentialsBuffer  } } ],
          [ { payload : { data : twitterCredentialsBuffer } } ], ] =
                await Promise.all(
                  [ gcpSecretManager.accessSecretVersion(
                      { name : githubSecretResourceName }),
                    gcpSecretManager.accessSecretVersion(
                      { name : twitterSecretResourceName }), ]);

      const
        githubAccessToken =
                githubCredentialsBuffer.toString(),
        [ twitterConsumerKey,
          twitterConsumerSecret,
          twitterToken,
          twitterTokenSecret, ] =
                twitterCredentialsBuffer.toString().split(';');

      // Initialize GitHub, Twitter API clients
      const
        github =
          new Octokit(
            { auth : githubAccessToken }),
        twitter =
          new Twitter(
            { consumer_key        : twitterConsumerKey,
              consumer_secret     : twitterConsumerSecret,
              access_token_key    : twitterToken,
              access_token_secret : twitterTokenSecret, });

      // Fetch tweets from Twitter collections
      // Fetch generated content on last update from GitHub
      const
        twitterCollectionsEntriesEndpoint = 'collections/entries';

      const
        [ { response : { timeline : regularEventsTimeline },
            objects  : { tweets   : regularEventsTweets }, },
          { response : { timeline : specialEventsTimeline },
            objects  : { tweets   : specialEventsTweets }, },
          { response : { timeline : bulletinsTimeline },
            objects  : { tweets   : bulletinsTweets }, },
          { data     :
              { content           : generatedContentJsonBase64,
                sha               : generatedContentSha1, }, }, ] =
                await Promise.all(
                  [ twitter.get(
                      twitterCollectionsEntriesEndpoint,
                      { id         :
                          `custom-${twitterRegularEventsCollectionId}`,
                        count      : twitterRegularEventsCollectionCount,
                        tweet_mode : 'extended', }),
                    twitter.get(
                      twitterCollectionsEntriesEndpoint,
                      { id         :
                          `custom-${twitterSpecialEventsCollectionId}`,
                        count      : twitterSpecialEventsCollectionCount,
                        tweet_mode : 'extended', }),
                    twitter.get(
                      twitterCollectionsEntriesEndpoint,
                      { id         : `custom-${twitterBulletinsCollectionId}`,
                        count      : twitterBulletinsCollectionCount,
                        tweet_mode : 'extended', }),
                    github.repos.getContents(
                      { owner : githubOwner,
                        repo  : githubRepository,
                        path  : githubGeneratedContentPath, }), ]);

      const
        generatedContentJson =
          Buffer.
                from(
                  generatedContentJsonBase64.replace(/\n/g, ''),
                  'base64').
                toString(),
        generatedContent =
          JSON.parse(generatedContentJson),
        { digests :
            [ regularEventsTimelineDigest,
              specialEventsTimelineDigest,
              bulletinsTimelineDigest, ], } = generatedContent;

      // Generate datasets for regular/special events and bulletins
      // Compute hashes to detect collection updates
      const
        regularEventsDataset      = [],
        specialEventsDataset      = [],
        bulletinsDataset          = [],
        regularEventsTimelineHash = createHash('md5'),
        specialEventsTimelineHash = createHash('md5'),
        bulletinsTimelineHash     = createHash('md5');

      for
        ( const
            [ timeline = [], tweets = {}, dataset, hash, transform ]
          of
            [ [ regularEventsTimeline, regularEventsTweets,
                regularEventsDataset, regularEventsTimelineHash,
                eventsTransform, ],
              [ specialEventsTimeline, specialEventsTweets,
                specialEventsDataset, specialEventsTimelineHash,
                eventsTransform, ],
              [ bulletinsTimeline, bulletinsTweets,
                bulletinsDataset, bulletinsTimelineHash,
                bulletinsTransform, ], ] ) {

        let
          element                    = null,
          lastNonContinuationElement = null;

        for
          ( const
              { tweet : { id : tweetId } }
            of
              timeline ) {

          const
            { [ tweetId ]   :
                { full_text : tweetText,
                  entities  :
                    { urls  : tweetLinks }, }, } = tweets;

          let
            tweetTextExpandedLinks = tweetText;

          for
            ( const
                { url          : linkShortened,
                  expanded_url : linkExpanded, }
              of
                tweetLinks )

            tweetTextExpandedLinks =
              tweetTextExpandedLinks.replace(linkShortened, linkExpanded);

          [ element,
            lastNonContinuationElement, ] =
                  transform(
                    tweetTextExpandedLinks,
                    lastNonContinuationElement);

          if (element)
            dataset.push(element);

          hash.update(tweetText);
        }
      }

      const
        [ regularEventsTimelineComputedDigest,
          specialEventsTimelineComputedDigest,
          bulletinsTimelineComputedDigest, ] =
                [ regularEventsTimelineHash.digest('hex'),
                  specialEventsTimelineHash.digest('hex'),
                  bulletinsTimelineHash.digest('hex'), ];

      // If at least one collection has updates, push datasets and digests
      // to GitHub to trigger site regeneration
      const
        regularEventsCollectionUpdated =
          regularEventsTimelineDigest !== regularEventsTimelineComputedDigest,
        specialEventsCollectionUpdated =
          specialEventsTimelineDigest !== specialEventsTimelineComputedDigest,
        bulletinsCollectionUpdated =
          bulletinsTimelineDigest !== bulletinsTimelineComputedDigest,
        collectionsUpdated =
          regularEventsCollectionUpdated ||
                specialEventsCollectionUpdated ||
                bulletinsCollectionUpdated;

      if (collectionsUpdated) {

        const updatedCollections = [];

        if (regularEventsCollectionUpdated)
          updatedCollections.push('regular events');
        if (specialEventsCollectionUpdated)
          updatedCollections.push('special events');
        if (bulletinsCollectionUpdated)
          updatedCollections.push('bulletins');

        const
          computedGeneratedContent =
            { regularEvents : regularEventsDataset,
              specialEvents : specialEventsDataset,
              bulletins     : bulletinsDataset,
              digests       :
                [ regularEventsTimelineComputedDigest,
                  specialEventsTimelineComputedDigest,
                  bulletinsTimelineComputedDigest, ], },
          computedGeneratedContentJson =
            JSON.stringify(computedGeneratedContent),
          computedGeneratedContentJsonBase64 =
            Buffer.
                  from(computedGeneratedContentJson).
                  toString('base64');

        const
          updatedCollectionsList =
            updatedCollections.join(', '),
          githubCommitMessage =
            'Generated content data: Updates in ' +
                  updatedCollectionsList;

        const
          { data : { commit : { sha : githubCommitSha1 } } } =
                  await github.repos.createOrUpdateFile(
                    { owner   : githubOwner,
                      repo    : githubRepository,
                      path    : githubGeneratedContentPath,
                      message : githubCommitMessage,
                      content : computedGeneratedContentJsonBase64,
                      sha     : generatedContentSha1, });

        console.log(
          `Commit ${githubCommitSha1.slice(0, 7)} - Updates in ` +
                updatedCollectionsList);
      }

      else
        console.log('No collection updates');
    }

    catch (error) {

      console.error('****** ERROR ******');

      if ('errors' in error)
        console.error('Twitter API error', error.errors);

      else
        console.error(error);
    }
  };
