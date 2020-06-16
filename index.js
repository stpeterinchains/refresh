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
    TWITTER_ANNOUNCEMENTS_COLLECTION_ID     : twitterAnnouncementsCollectionId,
    TWITTER_ANNOUNCEMENTS_COLLECTION_COUNT  :
            twitterAnnouncementsCollectionCount,
    TWITTER_REGULAR_EVENTS_COLLECTION_ID    : twitterRegularEventsCollectionId,
    TWITTER_REGULAR_EVENTS_COLLECTION_COUNT :
            twitterRegularEventsCollectionCount,
    TWITTER_SPECIAL_EVENTS_COLLECTION_ID    : twitterSpecialEventsCollectionId,
    TWITTER_SPECIAL_EVENTS_COLLECTION_COUNT :
            twitterSpecialEventsCollectionCount,
    TWITTER_BULLETINS_COLLECTION_ID         : twitterBulletinsCollectionId,
    TWITTER_BULLETINS_COLLECTION_COUNT      : twitterBulletinsCollectionCount,
  } = process.env;

// Define tweet transform and support functions
const

  /**
   * Transforms an announcement tweet into an announcement object.
   * Handles continuation tweets (title not present), which append their
   * descriptive text to that of the last non-continuation tweet.
   *
   * @function announcementsTransform
   * @param {string} tweetId - Announcement tweet status id.
   * @param {string} tweetText - Announcement tweet raw text.
   * @param {(object|null)} lastNonContinuationAnnouncement - Last announcement object.
   * @return {object[3]} - Transform result: Announcement object, last announcement object, error descriptor.
   */

  announcementsTransform =
    (tweetId, tweetText, lastNonContinuationAnnouncement) => {

      try {

        const
          announcementDocument =
            yamlSafeLoad(
              tweetText,
              { schema : yamlFailsafeSchema });

        const
          { title,                       // required, undefined if con tweet
            sub  : subtitle,             // optional
            color,                       // optional
            youtube,                     // optional
            tweet,                       // optional
            desc : descriptiveRaw = '',  // optional, required if con tweet
          } = announcementDocument;

        const
          descriptive  = descriptiveRaw.trim(),
          continuation = ! title && descriptive;

        if (! continuation) {

          if (! title)
            throw new TypeError('Title missing');

          const
            announcement =
              { title, subtitle, color, youtube, tweet, descriptive };

          return [ announcement, announcement, null ];
        }

        else
          return handleContinuation(
            lastNonContinuationAnnouncement,
            descriptive);
      }

      catch (error) {

        return prepare(error, tweetId);
      }
    },

  /**
   * Transforms a regular or special event tweet into an event object.
   * Handles continuation tweets (title not present), which append their
   * descriptive text to that of the last non-continuation tweet.
   *
   * @function eventsTransform
   * @param {string} tweetId - Event tweet status id.
   * @param {string} tweetText - Event tweet raw text.
   * @param {(object|null)} lastNonContinuationEvent - Last event object.
   * @return {object[3]} - Transform result: Event object, last event object, error descriptor.
   */

  eventsTransform =
    (tweetId, tweetText, lastNonContinuationEvent) => {

      try {

        const
          eventDocument =
            yamlSafeLoad(
              tweetText,
              { schema : yamlFailsafeSchema });

        const
          { title,                       // required, undefined if con tweet
            sub  : subtitle,             // optional
            color,                       // optional
            loc  : location,             // optional
            times                 = [],  // optional
            desc : descriptiveRaw = '',  // optional, required if con tweet
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

        else
          return handleContinuation(
            lastNonContinuationEvent,
            descriptive);
      }

      catch (error) {

        return prepare(error, tweetId);
      }
    },

  /**
   * Transforms a bulletin tweet into a bulletin object.
   * Note: Unlike annoucements and events, bulletins have no continuations.
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
          bulletinDocument =
            yamlSafeLoad(
              tweetText,
              { schema : yamlFailsafeSchema });

        const
          { date,            // required
            title,           // required
            sub : subtitle,  // optional
            link,            // required
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

        return prepare(error);
      }
    },

  /**
   * Handles a continuation tweet.
   *
   * @function handleContinuation
   * @param {object} lastNonContinuation - Last element object.
   * @param {string} descriptive - Trimmed descriptive text from continuation tweet.
   * @return {object[3]} - Transform result: Element object (always null), last element object, error descriptor (always null).
   */

  handleContinuation =
    (lastNonContinuation, descriptive) => {

      if (lastNonContinuation) {

        const separator =
          descriptive ?
                '\n\n' :
                '';

        lastNonContinuation.descriptive +=
          separator +
                descriptive;

        return [ null, lastNonContinuation, null ];
      }

      else
        throw new Error('Continuation tweet with no primary');
    },

  /**
   * Prepares a transform's error result.
   *
   * @function
   * @param {(YAMLException|TypeError|Error)} error - Error object caught in transform.
   * @return {object[3]} - Transform result: Element object (always null), last element object (always null), error descriptor.
   */

  prepare =
    (error, tweetId) => {

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
    };

// Define module export function

/**
 * Google Cloud Function which reads tweets from each of four Twitter
 * collections containing small YAML documents describing announcements,
 * regular events, special events, and bulletins to be displayed by the
 * accompanying website. Tweet contents are transformed into objects and
 * committed to the website's GitHub repo as a single JSON file. The
 * website is a Jekyll site hosted on GitHub Pages; the commit automatically
 * triggers the Jekyll build process to refresh the site with the new data.
 *
 * @async
 * @function agent
 * @param {object} message - GCP PubsubMessage object.
 * @param {object} context - GCP Function context object. Ignored.
 * @see {@link https://cloud.google.com/functions/docs/writing/background}
 * @see {@link https://cloud.google.com/functions/docs/calling/pubsub}
 */

exports.agent =
  async message => {

    // Define invokation options
    let
      dryRun = false;

    try {

      const
        { attributes :
            { dryRun : dryRunOption = false }, } = message;

      dryRun =
        !! JSON.parse(dryRunOption);
    }

    catch (error) {

      // keep default options
    }

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
        [ { response : { timeline : announcementsTimeline },
            objects  : { tweets   : announcementsTweets }, },
          { response : { timeline : regularEventsTimeline },
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
                          `custom-${twitterAnnouncementsCollectionId}`,
                        count      : +twitterAnnouncementsCollectionCount,
                        tweet_mode : 'extended', }),
                    twitter.get(
                      twitterCollectionsEntriesEndpoint,
                      { id         :
                          `custom-${twitterRegularEventsCollectionId}`,
                        count      : +twitterRegularEventsCollectionCount,
                        tweet_mode : 'extended', }),
                    twitter.get(
                      twitterCollectionsEntriesEndpoint,
                      { id         :
                          `custom-${twitterSpecialEventsCollectionId}`,
                        count      : +twitterSpecialEventsCollectionCount,
                        tweet_mode : 'extended', }),
                    twitter.get(
                      twitterCollectionsEntriesEndpoint,
                      { id         : `custom-${twitterBulletinsCollectionId}`,
                        count      : +twitterBulletinsCollectionCount,
                        tweet_mode : 'extended', }),
                    github.repos.getContent(
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
            [ announcementsTimelineDigest,
              regularEventsTimelineDigest,
              specialEventsTimelineDigest,
              bulletinsTimelineDigest, ], } = generatedContent;

      // Generate datasets for regular/special events and bulletins
      // Compute hashes to detect collection updates
      const
        announcementsDataset      = [],
        regularEventsDataset      = [],
        specialEventsDataset      = [],
        bulletinsDataset          = [],
        announcementsErrors       = [],
        regularEventsErrors       = [],
        specialEventsErrors       = [],
        bulletinsErrors           = [],
        announcementsTimelineHash = createHash('md5'),
        regularEventsTimelineHash = createHash('md5'),
        specialEventsTimelineHash = createHash('md5'),
        bulletinsTimelineHash     = createHash('md5');

      for
        ( const
            [ timeline = [], tweets = {},
              dataset,       errors,
              hash,          transform, ]
          of
            [ [ announcementsTimeline,     announcementsTweets,
                announcementsDataset,      announcementsErrors,
                announcementsTimelineHash, announcementsTransform, ],
              [ regularEventsTimeline,     regularEventsTweets,
                regularEventsDataset,      regularEventsErrors,
                regularEventsTimelineHash, eventsTransform, ],
              [ specialEventsTimeline,     specialEventsTweets,
                specialEventsDataset,      specialEventsErrors,
                specialEventsTimelineHash, eventsTransform, ],
              [ bulletinsTimeline,         bulletinsTweets,
                bulletinsDataset,          bulletinsErrors,
                bulletinsTimelineHash,     bulletinsTransform, ], ] ) {

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
        [ announcementsTimelineComputedDigest,
          regularEventsTimelineComputedDigest,
          specialEventsTimelineComputedDigest,
          bulletinsTimelineComputedDigest, ] =
                [ announcementsTimelineHash.digest('hex'),
                  regularEventsTimelineHash.digest('hex'),
                  specialEventsTimelineHash.digest('hex'),
                  bulletinsTimelineHash.digest('hex'), ];

      // If dry run, just log generated data; otherwise, if changes detected in
      // at least one collection, push generated data to GitHub to trigger
      // site regeneration

      const
        announcementsCollectionUpdated =
          announcementsTimelineDigest !== announcementsTimelineComputedDigest,
        regularEventsCollectionUpdated =
          regularEventsTimelineDigest !== regularEventsTimelineComputedDigest,
        specialEventsCollectionUpdated =
          specialEventsTimelineDigest !== specialEventsTimelineComputedDigest,
        bulletinsCollectionUpdated =
          bulletinsTimelineDigest !== bulletinsTimelineComputedDigest,
        collectionsUpdated =
          announcementsCollectionUpdated ||
                regularEventsCollectionUpdated ||
                specialEventsCollectionUpdated ||
                bulletinsCollectionUpdated;

      if (collectionsUpdated || dryRun) {

        const
          computedGeneratedContent =
            { announcements : announcementsDataset,
              regularEvents : regularEventsDataset,
              specialEvents : specialEventsDataset,
              bulletins     : bulletinsDataset,
              digests       :
                [ announcementsTimelineComputedDigest,
                  regularEventsTimelineComputedDigest,
                  specialEventsTimelineComputedDigest,
                  bulletinsTimelineComputedDigest, ],
              announcementsErrors,
              regularEventsErrors,
              specialEventsErrors,
              bulletinsErrors, },
          computedGeneratedContentJson =
            JSON.stringify(computedGeneratedContent),
          computedGeneratedContentJsonBase64 =
            Buffer.
                  from(computedGeneratedContentJson).
                  toString('base64');

        if (! dryRun) {

          const
            updatedCollections = [];

          if (announcementsCollectionUpdated)
            updatedCollections.push('announcements');
          if (regularEventsCollectionUpdated)
            updatedCollections.push('regular events');
          if (specialEventsCollectionUpdated)
            updatedCollections.push('special events');
          if (bulletinsCollectionUpdated)
            updatedCollections.push('bulletins');

          const
            updatedCollectionsList =
              updatedCollections.join(', '),
            githubCommitMessage =
              'Generated content data: Updates in ' +
                    updatedCollectionsList;

          const
            { data : { commit : { sha : githubCommitSha1 } } } =
                    await github.repos.createOrUpdateFileContents(
                      { owner   : githubOwner,
                        repo    : githubRepository,
                        path    : githubGeneratedContentPath,
                        message : githubCommitMessage,
                        content : computedGeneratedContentJsonBase64,
                        sha     : generatedContentSha1, });

          console.info(
            `Commit ${githubCommitSha1.slice(0, 7)} - Updates in ` +
                  updatedCollectionsList);
        }

        else
          console.info(computedGeneratedContentJson);
      }

      else
        console.info('No collection updates');
    }

    catch (error) {

      console.error('****** ERROR ******');

      if ('errors' in error)
        console.error('Twitter API error', error.errors);

      else
        console.error(error);
    }
  };
