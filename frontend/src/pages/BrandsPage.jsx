import { useState, useEffect, useCallback } from 'react';
import { api } from '../utils/api';
import BrandList from './brands/BrandList';
import CampaignSummary from './brands/CampaignSummary';
import EntryTables from './brands/EntryTables';
import EntryDetail from './brands/EntryDetail';

/**
 * Brand Management — 4-level drill-down:
 *   Level 1: Brand cards with FY-grouped campaigns
 *   Level 2: Campaign summary dashboard (aggregated metrics)
 *   Level 3: Platform-specific entry tables (Instagram, YouTube, etc.)
 *   Level 4: Expanded entry detail (demographics, caption, comments, transcript)
 */
export default function BrandsPage() {
  const [level, setLevel] = useState(1);
  const [brands, setBrands] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedBrand, setSelectedBrand] = useState(null);
  const [selectedCampaign, setSelectedCampaign] = useState(null);
  const [selectedEntry, setSelectedEntry] = useState(null);
  const [selectedPlatform, setSelectedPlatform] = useState(null);
  const [campaigns, setCampaigns] = useState([]);
  const [entries, setEntries] = useState([]);

  // Fetch partnerships from API
  const loadBrands = useCallback(async () => {
    try {
      const data = await api.get('/api/partnerships');
      // Map PoC fields into the brand_poc array BrandList expects
      const mapped = (data || []).map((b) => ({
        ...b,
        brand_poc: [b.brand_poc_1, b.brand_poc_2, b.brand_poc_3].filter(Boolean),
      }));
      setBrands(mapped);
    } catch (err) {
      console.error('Failed to load brands:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadBrands(); }, [loadBrands]);

  // Fetch campaigns for a specific partnership
  const loadCampaigns = useCallback(async (partnershipId) => {
    try {
      const data = await api.get(`/api/partnerships/${partnershipId}/campaigns`);
      setCampaigns(data || []);
      return data || [];
    } catch (err) {
      console.error('Failed to load campaigns:', err);
      return [];
    }
  }, []);

  // Fetch entries for a specific campaign
  const loadEntries = useCallback(async (campaignId) => {
    try {
      const data = await api.get(`/api/campaigns/${campaignId}/entries`);
      setEntries(data || []);
      return data || [];
    } catch (err) {
      console.error('Failed to load entries:', err);
      return [];
    }
  }, []);

  const drillToCampaign = async (brand, campaign) => {
    setSelectedBrand(brand);
    setSelectedCampaign(campaign);
    await loadEntries(campaign.id);
    setLevel(2);
  };

  const drillToEntries = () => setLevel(3);

  const drillToDetail = (entry, platform) => {
    setSelectedEntry(entry);
    setSelectedPlatform(platform);
    setLevel(4);
  };

  const goBack = (toLevel) => {
    setLevel(toLevel);
    if (toLevel <= 3) setSelectedEntry(null);
    if (toLevel <= 1) {
      setSelectedCampaign(null);
      setSelectedBrand(null);
      loadBrands(); // Refresh list
    }
  };

  // Aggregate metrics from entries
  const metrics = entries.length > 0 ? {
    totalReach: entries.reduce((s, e) => s + (e.reach || 0), 0),
    totalViews: entries.reduce((s, e) => s + (e.views || 0), 0),
    totalLikes: entries.reduce((s, e) => s + (e.likes || 0), 0),
    totalComments: entries.reduce((s, e) => s + (e.comments_count || 0), 0),
    totalSpend: entries.reduce((s, e) => s + (e.amount || 0), 0),
    entryCount: entries.length,
    liveCount: entries.filter(e => e.content_link).length,
  } : null;

  if (loading) {
    return (
      <div className="page-container" style={{ textAlign: 'center', padding: 60, color: 'var(--text-muted)' }}>
        Loading brands…
      </div>
    );
  }

  return (
    <div className="page-container">
      {/* Breadcrumb */}
      <div className="breadcrumb">
        <button className={`breadcrumb-item${level === 1 ? ' active' : ''}`} onClick={() => goBack(1)}>
          Brands
        </button>
        {level >= 2 && (
          <>
            <span className="breadcrumb-sep">›</span>
            <button className={`breadcrumb-item${level === 2 ? ' active' : ''}`} onClick={() => goBack(2)}>
              {selectedBrand?.brand_name}
            </button>
          </>
        )}
        {level >= 3 && (
          <>
            <span className="breadcrumb-sep">›</span>
            <button className={`breadcrumb-item${level === 3 ? ' active' : ''}`} onClick={() => goBack(3)}>
              {selectedCampaign?.campaign_name}
            </button>
          </>
        )}
        {level === 4 && (
          <>
            <span className="breadcrumb-sep">›</span>
            <span className="breadcrumb-item active">@{selectedEntry?.creator_username}</span>
          </>
        )}
      </div>

      {level === 1 && (
        <BrandList brands={brands} onSelectCampaign={drillToCampaign} onRefresh={loadBrands} />
      )}

      {level === 2 && selectedCampaign && (
        <CampaignSummary
          brand={selectedBrand}
          campaign={selectedCampaign}
          metrics={metrics}
          onViewEntries={drillToEntries}
          onRefreshEntries={() => loadEntries(selectedCampaign.id)}
        />
      )}

      {level === 3 && entries && (
        <EntryTables
          campaign={selectedCampaign}
          entries={entries}
          onSelectEntry={drillToDetail}
        />
      )}

      {level === 4 && selectedEntry && (
        <EntryDetail entry={selectedEntry} platform={selectedPlatform} />
      )}
    </div>
  );
}
