from travelassist.config import load_kpis, load_settings


def test_load_settings():
    settings = load_settings()
    assert "project" in settings
    assert settings["project"]["name"] == "TravelAssist"


def test_load_kpis():
    kpis = load_kpis()
    assert "kpis" in kpis
    assert len(kpis["kpis"]) >= 5