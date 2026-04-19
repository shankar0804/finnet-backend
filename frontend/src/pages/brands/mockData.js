/** Mock data for Brand Management UI development */

export const MOCK_BRANDS = [
  {
    id: 'b1',
    brand_name: 'boAt Lifestyle',
    brand_poc: ['Rahul Mehta', 'Priyanka Singh', ''],
    finnet_poc: 'shankar@finnetmedia.com',
    status: 'active',
    campaigns: [
      {
        id: 'c1', campaign_name: 'Summer Vibes Drop', month: 'Jun', year: 2026, fy: 'FY 26-27',
        platforms: 'Instagram,YouTube', status: 'active', budget: 850000,
        start_date: '2026-06-01', end_date: '2026-06-30',
      },
      {
        id: 'c2', campaign_name: 'Airdopes Launch', month: 'Apr', year: 2026, fy: 'FY 26-27',
        platforms: 'Instagram', status: 'completed', budget: 1200000,
        start_date: '2026-04-01', end_date: '2026-04-20',
      },
      {
        id: 'c3', campaign_name: 'Diwali Beats', month: 'Oct', year: 2025, fy: 'FY 25-26',
        platforms: 'YouTube', status: 'completed', budget: 2000000,
        start_date: '2025-10-10', end_date: '2025-11-05',
      },
      {
        id: 'c4', campaign_name: 'Republic Day Sale', month: 'Jan', year: 2026, fy: 'FY 25-26',
        platforms: 'Instagram', status: 'completed', budget: 600000,
        start_date: '2026-01-20', end_date: '2026-01-31',
      },
    ],
  },
  {
    id: 'b2',
    brand_name: 'Mamaearth',
    brand_poc: ['Neha Kapoor', 'Amit Gupta', 'Sara Jain'],
    finnet_poc: 'riya@finnetmedia.com',
    status: 'active',
    campaigns: [
      {
        id: 'c5', campaign_name: 'Onion Hair Oil Push', month: 'May', year: 2026, fy: 'FY 26-27',
        platforms: 'Instagram,LinkedIn', status: 'active', budget: 500000,
        start_date: '2026-05-01', end_date: '2026-05-31',
      },
      {
        id: 'c6', campaign_name: 'Vitamin C Serum', month: 'Mar', year: 2026, fy: 'FY 25-26',
        platforms: 'YouTube', status: 'completed', budget: 750000,
        start_date: '2026-03-01', end_date: '2026-03-25',
      },
    ],
  },
  {
    id: 'b3',
    brand_name: 'Noise',
    brand_poc: ['Gaurav Sinha', '', ''],
    finnet_poc: 'shankar@finnetmedia.com',
    status: 'paused',
    campaigns: [
      {
        id: 'c7', campaign_name: 'ColorFit Ultra Launch', month: 'Feb', year: 2026, fy: 'FY 25-26',
        platforms: 'Instagram', status: 'completed', budget: 400000,
        start_date: '2026-02-01', end_date: '2026-02-28',
      },
    ],
  },
];

/** Generate platform-specific entries for a campaign */
export function getMockEntries(campaignId) {
  const ENTRIES = {
    c1: { // boAt Summer Vibes — Instagram
      instagram: [
        {
          id: 'e1', username: 'dhruvbatra.ai', profile_link: 'https://instagram.com/dhruvbatra.ai',
          followers: 1200000, deliverable: 'Reel', commercials: 85000, avg_views_est: 450000,
          utm_link: 'https://boat.com/utm/dhruv-summer', timestamp: '2026-06-05T10:30:00Z',
          live_link: 'https://instagram.com/reel/abc123',
          video_views: 520000, play_count: 680000, likes: 42000, comments: 1800,
          shares: 3200, saves: 8500, engagement_rate: 4.2, duration_secs: 32, avd: '18.5s', skip_rate: '12%',
          demographics: { age_13_17: '8%', age_18_24: '42%', age_25_34: '32%', age_35_44: '12%', age_45_54: '6%', male: '55%', female: '45%', cities: ['Mumbai', 'Delhi', 'Bangalore', 'Pune', 'Hyderabad'] },
          caption: '🔥 This summer is all about the vibes! boAt Airdopes are my daily essential. Use code DHRUV10 for 10% off! #boAtLifestyle #SummerVibes #Ad',
          hashtags: ['#boAtLifestyle', '#SummerVibes', '#Airdopes', '#Ad', '#Collab'],
          comments_list: [
            { user: 'tech_rahul', text: 'These look amazing! 🔥', likes: 45, time: '2h ago' },
            { user: 'gadget_girl', text: 'Just ordered mine!', likes: 23, time: '3h ago' },
            { user: 'musiclover99', text: 'Sound quality is insane', likes: 18, time: '5h ago' },
          ],
          transcript: 'Hey guys! So I have been using these new boAt Airdopes for the past week and honestly the sound quality is just next level...',
        },
        {
          id: 'e2', username: 'shubhnaruka', profile_link: 'https://instagram.com/shubhnaruka',
          followers: 850000, deliverable: 'Reel', commercials: 65000, avg_views_est: 320000,
          utm_link: 'https://boat.com/utm/shubh-summer', timestamp: '2026-06-08T14:00:00Z',
          live_link: 'https://instagram.com/reel/def456',
          video_views: 380000, play_count: 490000, likes: 31000, comments: 1200,
          shares: 2100, saves: 6200, engagement_rate: 3.8, duration_secs: 28, avd: '16.2s', skip_rate: '15%',
          demographics: { age_13_17: '12%', age_18_24: '38%', age_25_34: '30%', age_35_44: '14%', age_45_54: '6%', male: '48%', female: '52%', cities: ['Delhi', 'Mumbai', 'Jaipur', 'Lucknow', 'Chennai'] },
          caption: 'POV: When your playlist hits different with boAt 🎧 #boAtLifestyle #MusicEveryday',
          hashtags: ['#boAtLifestyle', '#MusicEveryday', '#Reel'],
          comments_list: [
            { user: 'vibes_only', text: 'Need these asap!! 😍', likes: 32, time: '1h ago' },
            { user: 'audio_nerd', text: 'How is the bass?', likes: 12, time: '4h ago' },
          ],
          transcript: 'You know that feeling when you put on your earbuds and the beat just hits right...',
        },
        {
          id: 'e3', username: 'ravikapoorirs', profile_link: 'https://instagram.com/ravikapoorirs',
          followers: 2100000, deliverable: 'Story', commercials: 45000, avg_views_est: 600000,
          utm_link: 'https://boat.com/utm/ravi-summer', timestamp: '2026-06-10T09:00:00Z',
          live_link: '',
          video_views: 0, play_count: 0, likes: 0, comments: 0,
          shares: 0, saves: 0, engagement_rate: 0, duration_secs: 15, avd: '', skip_rate: '',
          demographics: null,
          caption: '', hashtags: [], comments_list: [], transcript: '',
        },
      ],
      youtube: [],
      linkedin: [],
      twitter: [],
      custom: [],
    },
    c2: { // boAt Airdopes Launch
      instagram: [
        {
          id: 'e4', username: 'thekuldeepshoww', profile_link: 'https://instagram.com/thekuldeepshoww',
          followers: 950000, deliverable: 'Reel', commercials: 70000, avg_views_est: 280000,
          utm_link: '', timestamp: '2026-04-05T12:00:00Z',
          live_link: 'https://instagram.com/reel/ghi789',
          video_views: 310000, play_count: 420000, likes: 25000, comments: 980,
          shares: 1500, saves: 4200, engagement_rate: 3.1, duration_secs: 45, avd: '22.1s', skip_rate: '18%',
          demographics: { age_13_17: '10%', age_18_24: '40%', age_25_34: '28%', age_35_44: '15%', age_45_54: '7%', male: '62%', female: '38%', cities: ['Mumbai', 'Delhi', 'Kolkata', 'Chennai', 'Ahmedabad'] },
          caption: 'The new Airdopes are HERE 🚀 #boAt #NewLaunch',
          hashtags: ['#boAt', '#NewLaunch', '#Airdopes'],
          comments_list: [],
          transcript: '',
        },
      ],
      youtube: [],
      linkedin: [],
      twitter: [],
      custom: [],
    },
    c3: { // boAt Diwali Beats — YouTube
      instagram: [],
      youtube: [
        {
          id: 'e5', username: 'TechnicalGuruji', profile_link: 'https://youtube.com/@TechnicalGuruji',
          subscribers: 23000000, deliverable: 'Video', commercials: 500000, avg_views_est: 2000000,
          utm_link: 'https://boat.com/utm/tg-diwali', timestamp: '2025-10-15T18:00:00Z',
          live_link: 'https://youtube.com/watch?v=xyz123',
          video_views: 2400000, impressions: 5200000, likes: 180000, comments: 12000,
          duration_secs: 620, engagement_rate: 3.8, avd: '4m 12s', apv: '65%', ctr: '8.2%',
        },
      ],
      linkedin: [],
      twitter: [],
      custom: [],
    },
  };
  return ENTRIES[campaignId] || { instagram: [], youtube: [], linkedin: [], twitter: [], custom: [] };
}

/** Aggregate metrics across all entries in a campaign */
export function aggregateMetrics(entries) {
  const all = [...entries.instagram, ...entries.youtube, ...entries.linkedin, ...entries.twitter, ...entries.custom];
  const live = all.filter(e => e.live_link);
  const totalViews = all.reduce((s, e) => s + (e.video_views || 0), 0);
  const totalLikes = all.reduce((s, e) => s + (e.likes || 0), 0);
  const totalComments = all.reduce((s, e) => s + (e.comments || 0), 0);
  const totalShares = all.reduce((s, e) => s + (e.shares || 0), 0);
  const totalSaves = all.reduce((s, e) => s + (e.saves || 0), 0);
  const totalBudget = all.reduce((s, e) => s + (e.commercials || 0), 0);
  const engRates = all.filter(e => e.engagement_rate > 0).map(e => e.engagement_rate);
  const avgEng = engRates.length ? (engRates.reduce((a, b) => a + b, 0) / engRates.length) : 0;
  const cpv = totalViews > 0 ? totalBudget / totalViews : 0;
  const totalEng = totalLikes + totalComments + totalShares + totalSaves;
  const cpe = totalEng > 0 ? totalBudget / totalEng : 0;

  // Platform breakdown
  const platforms = {};
  for (const [plat, arr] of Object.entries(entries)) {
    if (arr.length > 0) {
      const types = {};
      arr.forEach(e => { types[e.deliverable] = (types[e.deliverable] || 0) + 1; });
      platforms[plat] = types;
    }
  }

  return {
    totalInfluencers: all.length, liveContent: live.length,
    totalViews, totalLikes, totalComments, totalShares, totalSaves,
    avgEngRate: avgEng.toFixed(2), totalBudget, cpv: cpv.toFixed(2), cpe: cpe.toFixed(2),
    platforms,
  };
}
