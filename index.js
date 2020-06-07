'use strict';

/** @module stpeterinchains-refresh */

// Import dependencies
const
  { createHash } = require('crypto'),
  { URL }        = require('url');

const
  { SecretManagerServiceClient }  = require('@google-cloud/secret-manager'),
  { Octokit }                     = require('@octokit/rest'),
  Twitter                         = require('twitter-lite');

const
  { YAMLException,
    safeLoad        : yamlSafeLoad,
    FAILSAFE_SCHEMA : yamlFailsafeSchema, } = require('js-yaml');

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
    TWITTER_BULLETINS_COLLECTION_COUNT      : twitterBulletinsCollectionCount,
  } = process.env;

// Define support functions
const

  /**
   * Transforms a raw regular/special event tweet into an event object.
   * Handles continuation tweets (title not present), which append their
   * descriptive text to that of the last non-continuation tweet.
   *
   * @function eventsTransform
   * @param {string} tweetId - Event tweet status id.
   * @param {string} tweetText - Event tweet raw text.
   * @param {(object|null)} lastNonContinuationEvent - Last primary event object.
   * @return {object[3]} - Transform result: Event object, last event object, error descriptor.
   */

  eventsTransform =
    (tweetId, tweetText, lastNonContinuationEvent) => {

      try {

        const
          eventDocument = yamlSafeLoad(
            tweetText,
            { schema : yamlFailsafeSchema });

        const
          { title,                       // undefined if continuation tweet
            sub  : subtitle,             // optional
            color,                       // optional
            loc  : location,             // optional
            times                 = [],  // optional
            desc : descriptiveRaw = '',  // optional, required if contn tweet
          } = eventDocument;

        const
          descriptive  = descriptiveRaw.trim(),
          continuation = ! title && descriptive;

        if (! continuation) {

          if (! title)
            throw new TypeError('Title missing');

          for (const { day, time } of times)
            if (! day || ! time)
              throw new TypeError('Day/date or time missing');

          const
            event = { title, subtitle, color, location, times, descriptive };

          return [ event, event, null ];
        }

        else {

          if (lastNonContinuationEvent) {

            const
              separator =
                descriptive ?
                      '\n\n' :
                      '';

            lastNonContinuationEvent.descriptive +=
              separator +
                    descriptive;

            return [ null, lastNonContinuationEvent, null ];
          }

          else
            throw new Error('Continuation tweet with no primary');
        }
      }

      catch (error) {

        let descriptor;

        if (error instanceof YAMLException)
          descriptor =
            { error  : 'Invalid YAML',
              type   : error.name,
              reason : error.reason,
              mark   : error.mark, };

        else if (error instanceof TypeError)
          descriptor =
            { error  : 'Invalid structure',
              type   : error.name,
              reason : error.message, };

        else
          descriptor =
            { error  : 'Unexpected error',
              type   : error.name,
              reason : error.message, };

        descriptor.tweetId = tweetId;

        console.error(descriptor);

        return [ null, null, descriptor ];
      }
    },

  /**
   * Transforms a raw bulletin tweet into a bulletin object.
   *
   * @function bulletinsTransform
   * @param {string} tweetId - Bulletin tweet status id.
   * @param {string} tweetText - Bulletin tweet raw text.
   * @return {object[3]} - Transform result: Bulletin object, last bulletin object (always null), error descriptor.
   */

  bulletinsTransform =
    (tweetId, tweetText) => {

      try {

        const
          bulletinDocument = yamlSafeLoad(
            tweetText,
            { schema : yamlFailsafeSchema });

        const
          { date,
            title,
            sub : subtitle,  // optional
            link,
            inserts = [],    // optional
          } = bulletinDocument;

        if (! date || ! title)
          throw new TypeError(
            'Bulletin date or title missing');

        try { new URL(link); }
        catch (error) {
          throw new TypeError('Bulletin link missing or invalid');
        }

        for (const { title, link } of inserts) {

          if (! title )
            throw new TypeError('Insert title missing');

          try { new URL(link); }
          catch (error) {
            throw new TypeError('Insert link missing or invalid');
          }
        }

        const
          bulletin = { date, title, subtitle, link, inserts };

        return [ bulletin, null, null ];
      }

      catch (error) {

        let descriptor;

        if (error instanceof YAMLException)
          descriptor =
            { error  : 'Invalid YAML',
              type   : error.name,
              reason : error.reason,
              mark   : error.mark, };

        else if (error instanceof TypeError)
          descriptor =
            { error  : 'Invalid structure',
              type   : error.name,
              reason : error.message, };

        else
          descriptor =
            { error  : 'Unexpected error',
              type   : error.name,
              reason : error.message, };

        descriptor.tweetId = tweetId;

        console.error(descriptor);

        return [ null, null, descriptor ];
      }
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
        regularEventsErrors       = [],
        specialEventsErrors       = [],
        bulletinsErrors           = [],
        regularEventsTimelineHash = createHash('md5'),
        specialEventsTimelineHash = createHash('md5'),
        bulletinsTimelineHash     = createHash('md5');

      for
        ( const
            [ timeline = [], tweets = {},
              dataset, errors,
              hash, transform, ]
          of
            [ [ regularEventsTimeline, regularEventsTweets,
                regularEventsDataset, regularEventsErrors,
                regularEventsTimelineHash, eventsTransform, ],
              [ specialEventsTimeline, specialEventsTweets,
                specialEventsDataset, specialEventsErrors,
                specialEventsTimelineHash, eventsTransform, ],
              [ bulletinsTimeline, bulletinsTweets,
                bulletinsDataset, bulletinsErrors,
                bulletinsTimelineHash, bulletinsTransform, ], ] ) {

        let
          element                    = null,
          lastNonContinuationElement = null,
          errorDescriptor            = null;

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
            lastNonContinuationElement,
            errorDescriptor, ] =
                  transform(
                    tweetId,
                    tweetTextExpandedLinks,
                    lastNonContinuationElement);

          if (element)
            dataset.push(element);

          if (errorDescriptor)
            errors.push(errorDescriptor);

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
          regularEventsCollectionUpdated || specialEventsCollectionUpdated ||
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
                  bulletinsTimelineComputedDigest, ],
              regularEventsErrors,
              specialEventsErrors,
              bulletinsErrors, },
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
